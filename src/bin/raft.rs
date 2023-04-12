use rand::Rng;
use serde::{Serialize, Deserialize};
use thiserror::Error;
use std::collections::{HashMap, HashSet};

use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration};
use chrono::{Utc, DateTime};
use log::{
    info,
    debug,
};



use maelstrom_raft::*;


#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Message {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Topology {
        topology: HashMap<String, Vec<String>>
    },
    TopologyOk,
    Read {
        key: usize
    },
    ReadOk {
        value: usize
    },
    Write {
        key: usize,
        value: usize
    },
    WriteOk,
    Cas {
        key: usize,
        from: usize,
        to: usize
    },
    CasOk,
    Error {
        code: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        text: Option<String>
    },

    RequestVote{
        term: usize,
        candidate_id: String,
        last_log_index: usize,
        last_log_term: usize
    },
    RequestVoteOk{
        term: usize,
        vote_granted: bool
    }
}

impl std::fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{:?}", self)
    }
}


#[derive(Debug, Clone, Default)]
pub struct State {
    state: HashMap<usize, usize>,
    pub cluster: Shared<ClusterMembership>,
    pub election: Shared<ElectionState>,
    pub log: Shared<Log>
}

impl State {

    pub fn new(cluster: Shared<ClusterMembership>, election: Shared<ElectionState>, log: Shared<Log>) -> Self {
        Self {
            cluster,
            election,
            log,
            ..Default::default()
        }
    }
    pub fn handle(&mut self, envelope: &Envelope<Message>) {
        match envelope.message() {
            Message::Init { node_id, node_ids } => {
                
                {
                    let mut guard = self.cluster.lock().unwrap();
                    guard.id = node_id;
                    guard.all_nodes = node_ids.into_iter().collect();
                }

                debug!(target: "maelstrom-rpc", "Received: {}", envelope);

                {
                    let mut election = self.election.lock().unwrap();
                    election.is_ready = true;
                }

                envelope.reply(Message::InitOk).send().unwrap();
            },
            Message::Topology { topology } => {

                let mut guard = self.cluster.lock().unwrap();
                debug!(target: "maelstrom-rpc", "Received: {}", envelope);

                guard.neighbor_map = 
                    topology
                    .into_iter()
                    .map(
                        |(k, v)| 
                            (k, v.into_iter().collect::<HashSet<_>>())
                        )
                    .collect();

                envelope.reply(Message::TopologyOk).send().unwrap();
            },
            Message::Read { key } => {
                debug!(target: "maelstrom-rpc", "Received: {}", envelope);
                if !self.state.contains_key(&key) {
                    let error_text = format!("Key {} not found.", key);
                    envelope.reply(Message::Error { code: 20, text: Some(error_text)}).send().unwrap();
                    return;
                }
                let value = *self.state.get(&key).unwrap();
                envelope.reply(Message::ReadOk { value }).send().unwrap();
            },
            Message::Write { key, value } => {
                debug!(target: "maelstrom-rpc", "Received: {}", envelope);
                self.state
                .entry(key)
                .and_modify(|v| *v = value)
                .or_insert(value);
                envelope.reply(Message::WriteOk).send().unwrap();
            },
            Message::Cas { key, from, to } => {

                debug!(target: "maelstrom-rpc", "Received: {}", envelope);
                if !self.state.contains_key(&key) {
                    let error_text = format!("Could not find key at CAS: {}", key);
                    envelope.reply(Message::Error { code: 20, text: Some(error_text) }).send().unwrap();
                    return;
                }
                
                let previous_value = *self.state.get(&key).unwrap();
                if  previous_value != from {
                    let error_text = format!("Expecting {}, but had {}", from, previous_value);
                    envelope.reply(Message::Error { code: 22, text: Some(error_text) }).send().unwrap();
                    return;
                }

                self.state.insert(key, to);
                envelope.reply(Message::CasOk).send().unwrap();
            },
            Message::RequestVote { term, candidate_id, last_log_index, last_log_term } => {
                info!(target: "raft-rpc", "Received: {}", envelope);
                // We received a request to vote from a candidate peer.
                // TODO: Handle it.
                let mut election = self.election.lock().unwrap();
                election.maybe_step_down(term).unwrap();

                let log = self.log.lock().unwrap();

                let mut grant: bool = false;

                if term < election.term {
                    info!(target: "raft-rpc", "Candidate term ({}) lower than ours ({}); NOT granting vote.", term, election.term);
                } else if last_log_term < log.last().term {
                    info!(
                        target: "raft-rpc", 
                        "Have log entries from term ({}), which is newer than remote term ({}); NOT granting vote.",
                        log.last().term,
                        last_log_term
                    );
                } else if let Some(voted_for) = election.voted_for.as_ref() {
                    info!(
                        target: "raft-rpc",
                        "Already voted for {}; NOT granting vote.",
                        voted_for
                    );
                } else if last_log_term == log.last().term && last_log_index < log.len() {
                    info!(
                        target: "raft-rpc",
                        "Our logs are both at term {}, but our log is {} and theirs is only {} long; NOT granting vote.",
                        log.last().term,
                        log.len(),
                        last_log_index
                    );
                }
                else {
                    info!(
                        target: "raft-rpc",
                        "Granting vote to {}",
                        candidate_id
                    );
                    grant = true;
                    election.voted_for = Some(candidate_id);
                    election.reset_election_deadline();
                }

                envelope.reply(Message::RequestVoteOk { term: election.term, vote_granted: grant }).send().unwrap();
            },
            Message::RequestVoteOk { term, vote_granted } => {
                info!(target: "raft-rpc", "Received: {}", envelope);

                let mut election = self.election.lock().unwrap();
                election.reset_step_down_deadline();
                election.maybe_step_down(term).unwrap();

                if election.kind == ElectionMember::Candidate
                    && election.term == term
                    && election.term == election.vote_requested_for_term
                    && vote_granted 
                {
                    election.votes.insert(envelope.source.clone());
                    info!(target: "election", "Have votes: {:?}", election.votes);
                }

                let total_nodes = {
                    let cluster = self.cluster.lock().unwrap();
                    cluster.all_nodes.len()
                };

                if election.votes.len() >= ElectionState::majority(total_nodes) && election.kind == ElectionMember::Candidate {
                    election.become_leader().unwrap();
                }
            },
            _ => {
                
            }
        }
    }
}


#[derive(Debug, Error)]
pub enum RaftError {
    #[error("Current term is {0} but asked to advance to {1} which is backwards and terms are monotonically increasing.")]
    TermCannotGoBackwards(usize, usize),
    #[error("Only a candidate can become a leader")]
    MustBeACandidateToBecomeALeader,
    #[error(transparent)]
    Maelstrom(#[from] EnvelopeBuilderError)
}


#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ElectionMember {
    #[default]
    Follower,
    Leader,
    Candidate
}

#[derive(Debug, Clone, Default)]
pub struct ElectionState {
    pub timeout: Duration,
    pub deadline: DateTime<Utc>,
    pub step_down_deadline: DateTime<Utc>,
    pub kind: ElectionMember,
    pub term: usize,
    pub vote_requested_for_term: usize,
    /// Store the votes we've received in the current term.
    pub votes: HashSet<String>,
    pub is_ready: bool,
    pub voted_for: Option<String>
}

impl ElectionState {
    pub fn new() -> Self {
        Self {
            timeout: Duration::from_secs(2),
            // just wait for 5 seconds before doing the election dance.
            // because we may not have received our topology until then.
            deadline: Utc::now(),
            step_down_deadline: Utc::now(),
            kind: ElectionMember::Follower,
            term: 0,
            votes: Default::default(),
            vote_requested_for_term: 0,
            is_ready: false,
            voted_for: None
        }
    }
    pub fn become_candidate(&mut self) -> Result<(), RaftError> {
        self.kind = ElectionMember::Candidate;
        self.advance_term(self.term + 1)?;
        self.reset_election_deadline();
        self.reset_step_down_deadline();
        info!(target: "election", "Became candidate for term: {}", self.term);
        Ok(())
    }

    pub fn become_leader(&mut self) -> Result<(), RaftError> {
        if self.kind != ElectionMember::Candidate {
            return Err(RaftError::MustBeACandidateToBecomeALeader);
        }

        self.kind = ElectionMember::Leader;
        self.reset_step_down_deadline();
        info!(target: "election", "Became a leader for term: {}", self.term);

        Ok(())
    }

    pub fn advance_term(&mut self, term: usize) -> Result<(), RaftError> {
        if self.term >= term {
            return Err(RaftError::TermCannotGoBackwards(self.term, term))
        }
        self.term = term;
        self.voted_for = None;
        Ok(())
    }

    pub fn maybe_step_down(&mut self, term: usize) -> Result<(), RaftError> {
        if self.term < term {
            info!(target: "election", "Stepping down because remote term ({}) is higher than our term ({})", term, self.term);
            self.advance_term(term)?;
            self.become_follower();
        }

        Ok(())
    }

    pub fn become_follower(&mut self) {
        self.kind = ElectionMember::Follower;
        info!(target: "election", "Became follower for term: {}", self.term);
        self.reset_election_deadline();
    }

    pub fn reset_election_deadline(&mut self) {
        let mut rng = rand::thread_rng();
        let scale: f64 = rng.gen_range(1.0..2.0);

        let delay_by: Duration = Duration::from_secs_f64(self.timeout.as_secs_f64() * scale);
        self.deadline = Utc::now().checked_add_signed(chrono::Duration::from_std(delay_by).unwrap()).unwrap();
        info!(target: "election", "Just reset the election deadline to {}", self.deadline);
    }

    pub fn reset_step_down_deadline(&mut self) {
        self.step_down_deadline = Utc::now() + chrono::Duration::from_std(self.timeout).unwrap();
    }


    pub fn request_votes(
        &mut self, 
        our_id: &str,
        other_nodes: &Vec<String>,
        last_log_index: usize,
        last_log_term: usize
    ) -> Result<(), RaftError> {

        let term = {
            self.vote_requested_for_term = self.term;
            self.term
        };
        info!(target: "election", "About to request for votes to our peers ({:?})", other_nodes);
        // Build the envelopes first. Only if all succeed should we send them all out.
        let envelopes = 
        other_nodes
        .iter()
        .map(|neighbor| {

            EnvelopeBuilder::default()
            .destination(neighbor)
            .source(our_id)
            .message(
                Message::RequestVote { term, candidate_id: our_id.to_string(), last_log_index, last_log_term }
            )
            .build()
            .map_err(RaftError::Maelstrom)
        })
        .collect::<Result<Vec<Envelope<Message>>, RaftError>>()?;

        for envelope in envelopes {
            envelope.send().unwrap();
        }

        Ok(())
    }


    #[inline(always)]
    pub const fn majority(n: usize) -> usize {
        match n % 2 == 0 {
            true => n / 2 + 1,
            false => (n - 1) / 2 + 1
        }
    }


}

pub type Shared<T> = Arc<Mutex<T>>;


#[derive(Debug, Clone, Default)]
pub struct LogEntry {
    pub term: usize,
    pub data: Option<Envelope<Message>>
}

impl LogEntry {
    pub fn new(term: usize, data: Option<Envelope<Message>>) -> Self {
        Self {
            term,
            data
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Log {
    entries: Vec<LogEntry>
}

impl Log {
    pub fn new() -> Self {
        Self {
            entries: vec![]
        }
    }
    pub fn init() -> Self {
        Self {
            entries: vec![LogEntry::new(0, None)]
        }
    }
    pub fn get(&self, index: usize) -> Option<&LogEntry> {
        self.entries.get(index - 1)
    }

    pub fn append(&mut self, entries: &[LogEntry]) {
        self.entries.extend_from_slice(entries);
        debug!(target: "log", "Current log after append: {:#?}", self.entries);
    }

    pub fn last(&self) -> &LogEntry {
        self.entries.last().unwrap()
    }
    pub fn last_mut(&mut self) -> &mut LogEntry 
    {
        let last_index = self.entries.len() - 1;
        self.entries.get_mut(last_index).unwrap()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}


#[derive(Debug, Clone, Default)]
pub struct ClusterMembership {
    pub id: String,
    pub all_nodes: HashSet<String>,
    pub neighbor_map: HashMap<String, HashSet<String>>,
}

impl ClusterMembership {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn other_nodes(&self) -> impl Iterator<Item=&String> {
        self.all_nodes.iter().filter(|&node| node != &self.id)
    }
}
#[derive(Debug)]
pub struct Node {
    pub state_machine: Shared<State>,
    pub msg_rx: Receiver<Envelope<Message>>,
    pub election: Shared<ElectionState>,
    pub log: Shared<Log>,
    pub cluster_membership: Shared<ClusterMembership>,
}



impl Node {
    pub fn new(rx: Receiver<Envelope<Message>>) -> Self {
        let cluster_membership: Shared<ClusterMembership> = Default::default();
        let election = Arc::new(Mutex::new(ElectionState::new()));
        let log = Arc::new(Mutex::new(Log::init()));
        Self {
            state_machine: Arc::new(Mutex::new(State::new(cluster_membership.clone(), election.clone(), log.clone()))),
            msg_rx: rx,
            log,
            election,
            cluster_membership
        }
    }
    pub fn run(&mut self) {
        let election = self.election.clone();
        let log = self.log.clone();
        let cluster_membership = self.cluster_membership.clone();

        let _election_handle = std::thread::spawn(move || {
            loop {
                let mut guard = election.lock().unwrap();
                if guard.deadline < Utc::now() && guard.is_ready {
                    if guard.kind != ElectionMember::Leader {
                        guard.become_candidate().unwrap();
                        
                        let (last_log_index, last_log_term) = {
                            let log_ = log.lock().unwrap();
                            (log_.len(), log_.last().term)
                        };

                        let (our_id, other_nodes) = {
                            let cluster = cluster_membership.lock().unwrap();
                            (cluster.id.clone(), cluster.other_nodes().cloned().collect::<Vec<_>>())
                        };
                        guard.request_votes(&our_id, &other_nodes, last_log_index, last_log_term).unwrap();
                        guard.voted_for = Some(our_id.to_string());

                    } else {
                        guard.reset_election_deadline();
                    }
                }
                drop(guard);

                let mut rng = rand::thread_rng();
                let extra_sleep = Duration::from_millis(rng.gen_range(0..10));
                sleep(Duration::from_millis(100) + extra_sleep);
            }
        });


        let election = self.election.clone();
        let _leader_stepdown_handle = std::thread::spawn(move || {
            loop {
                sleep(Duration::from_millis(100));
                let mut guard = election.lock().unwrap();
                if guard.kind == ElectionMember::Leader && guard.step_down_deadline < Utc::now() {
                    info!(target: "election", "Stepping down: haven't received any acks recently.");
                    guard.become_follower();
                }
                drop(guard);
            }
        });

        while let Ok(envelope) = self.msg_rx.recv() {
            self.state_machine.lock().unwrap().handle(&envelope);
        }
    }
}

pub fn main() {
    let logger = simple_logger::SimpleLogger::new();
    logger.with_level(log::LevelFilter::Info).env().init().unwrap();

    let (stdin_tx, stdin_rx) = std::sync::mpsc::channel();

    std::thread::spawn(move || {
        for input in std::io::stdin().lines() {
            let line = input.unwrap();
            let deserialized: Envelope<Message> = serde_json::from_str(&line).unwrap();
            stdin_tx.send(deserialized).unwrap();
        }
    });

    let mut node = Node::new(stdin_rx);
    node.run();
}