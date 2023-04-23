use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

use chrono::{DateTime, Utc};
use log::{debug, info};
use std::sync::mpsc::Receiver;
use std::sync::{Arc};
// use tracing_mutex::stdsync::{
//     TracingMutex as Mutex
// };
use tracing_mutex::stdsync::tracing::Mutex;
use std::thread::sleep;
use std::time::Duration;

use maelstrom_raft::*;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Message {
    Init {
        node_id: NodeId,
        node_ids: Vec<NodeId>,
    },
    InitOk,
    Topology {
        topology: HashMap<NodeId, Vec<NodeId>>,
    },
    TopologyOk,
    Read {
        key: usize,
    },
    ReadOk {
        value: usize,
    },
    Write {
        key: usize,
        value: usize,
    },
    WriteOk,
    Cas {
        key: usize,
        from: usize,
        to: usize,
    },
    CasOk,
    Error {
        code: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        text: Option<String>,
    },
    RequestVote {
        term: usize,
        candidate_id: NodeId,
        last_log_index: usize,
        last_log_term: usize,
    },
    RequestVoteOk {
        term: usize,
        vote_granted: bool,
    },
    AppendEntries {
        term: usize,
        leader_id: NodeId,
        previous_log_index: usize,
        previous_log_term: usize,
        entries: Vec<LogEntry>,
        leader_commit_index: usize
    },
    AppendEntriesOk {
        term: usize,
        success: bool,
        previous_log_index: usize,
        entries_size: usize
    }
}

impl std::fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone, Default)]
pub struct RaftRuntime {
    state: HashMap<usize, usize>,
    pub cluster: Shared<ClusterMembership>,
    pub election: Shared<ElectionState>,
    pub leader_state: Shared<LeaderState>,
    pub log: Shared<Log>,
}

impl RaftRuntime {
    pub fn new(
        cluster: Shared<ClusterMembership>,
        election: Shared<ElectionState>,
        log: Shared<Log>,
        leader_state: Shared<LeaderState>
    ) -> Self {
        Self {
            cluster,
            election,
            log,
            leader_state,
            ..Default::default()
        }
    }
    pub fn handle(&mut self, envelope: &Envelope<Message>) {
        
        let mut election_guard = self.election.lock().unwrap();
        let mut leader_state_guard = self.leader_state.lock().unwrap();
        let mut log_guard = self.log.lock().unwrap();
        let mut cluster_guard = self.cluster.lock().unwrap();

        match envelope.message() {
            Message::Init { node_id, node_ids } => {
                {
                    cluster_guard.id = node_id;
                    cluster_guard.all_nodes = node_ids.into_iter().collect();
                }

                debug!(target: "maelstrom-rpc", "Received: {}", envelope);

                {
                    election_guard.is_ready = true;
                }

                envelope.reply(Message::InitOk).send().unwrap();
            }
            Message::Topology { topology } => {
                debug!(target: "maelstrom-rpc", "Received: {}", envelope);

                cluster_guard.neighbor_map = topology
                    .into_iter()
                    .map(|(k, v)| (k, v.into_iter().collect::<HashSet<_>>()))
                    .collect();

                envelope.reply(Message::TopologyOk).send().unwrap();
            }
            Message::Read { key } => {
                debug!(target: "maelstrom-rpc", "Received: {}", envelope);

                // If we're not a leader, reject the request.
                {
                    if !election_guard.is_leader() {
                        let reply =
                        envelope
                            .reply(Message::Error {
                                code: MaelstromError::TemporarilyUnavailable as usize,
                                text: Some("can't read because i'm not a leader".to_string()),
                            });
                        
                        debug!(target: "maelstrom-rpc", "Sending: {}", reply);
                        reply.send().unwrap();

                        return;
                    }
                    {
                        // Append to log before processing the message.
                        let term = election_guard.term;
                        log_guard.append(&[LogEntry { term, data: Some(envelope.message()) }]);
                    }
                }


                if !self.state.contains_key(&key) {
                    let error_text = format!("Key {} not found.", key);
                    let reply =
                    envelope
                        .reply(Message::Error {
                            code: MaelstromError::KeyDoesNotExist as usize,
                            text: Some(error_text),
                        });

                    debug!(target: "maelstrom-rpc", "Sending: {}", reply);
                    reply.send().unwrap();
                    return;
                }
                let value = *self.state.get(&key).unwrap();
                envelope.reply(Message::ReadOk { value }).send().unwrap();
                debug!(target: "maelstrom-rpc", "Sending: {}", envelope);
            }
            Message::Write { key, value } => {
                debug!(target: "maelstrom-rpc", "Received: {}", envelope);

                // If we're not a leader, reject the request.
                {
                    if !election_guard.is_leader() {
                        let reply =
                        envelope
                            .reply(Message::Error {
                                code: MaelstromError::TemporarilyUnavailable as usize,
                                text: Some("can't write because I'm not a leader".to_string()),
                            });
                        debug!(target: "maelstrom-rpc", "Sending: {}", reply);
                        reply.send().unwrap();
                        return;
                    }
                    {
                        // Append to log before processing the message.
                        let term = election_guard.term;
                        log_guard.append(&[LogEntry { term, data: Some(envelope.message()) }]);
                    }
                }
                self.state
                    .entry(key)
                    .and_modify(|v| *v = value)
                    .or_insert(value);
                let reply = envelope.reply(Message::WriteOk);
                reply.send().unwrap();

                debug!(target: "maelstrom-rpc", "Sending: {}", reply);
            }
            Message::Cas { key, from, to } => {
                debug!(target: "maelstrom-rpc", "Received: {}", envelope);

                // If we're not a leader, reject the request.
                {
                    if !election_guard.is_leader() {
                        let reply =
                        envelope
                            .reply(Message::Error {
                                code: MaelstromError::TemporarilyUnavailable as usize,
                                text: Some("can't cas because I'm not a leader".to_string()),
                            });
                        debug!(target: "maelstrom-rpc", "Sending: {}", reply);
                        reply.send().unwrap();
                        return;
                    }
                    {
                        info!(target: "append-log", "We're a leader so we'll append this entry to our log.");
                        // Append to log before processing the message.
                        let term = election_guard.term;
                        log_guard.append(&[LogEntry { term, data: Some(envelope.message()) }]);
                        info!(target: "append-log", "Entry append successful. Current log: {:#?}", log_guard.entries);
                    }
                }

                if !self.state.contains_key(&key) {
                    let error_text = format!("Could not find key at CAS: {}", key);
                    let reply =
                    envelope
                        .reply(Message::Error {
                            code: MaelstromError::KeyDoesNotExist as usize,
                            text: Some(error_text),
                        });
                    reply.send().unwrap();
                    debug!(target: "maelstrom-rpc", "Sending: {}", reply);
                    return;
                }

                let previous_value = *self.state.get(&key).unwrap();
                if previous_value != from {
                    let error_text = format!("Expecting {}, but had {}", from, previous_value);
                    let reply =
                    envelope
                        .reply(Message::Error {
                            code: MaelstromError::PreconditionFailed as usize,
                            text: Some(error_text),
                        });
                    
                    reply.send().unwrap();
                    debug!(target: "maelstrom-rpc", "Sending: {}", reply);
                    return;
                }

                self.state.insert(key, to);
                let reply =envelope.reply(Message::CasOk);
                reply.send().unwrap();
                debug!(target: "maelstrom-rpc", "Sending: {}", reply);
            }
            Message::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                info!(target: "raft-rpc", "Received: {}", envelope);
                election_guard.maybe_step_down(term).unwrap();

                let mut grant: bool = false;

                if term < election_guard.term {
                    info!(target: "raft-rpc", "Candidate term ({}) lower than ours ({}); NOT granting vote.", term, election_guard.term);
                } else if last_log_term < log_guard.last().term {
                    info!(
                        target: "raft-rpc",
                        "Have log entries from term ({}), which is newer than remote term ({}); NOT granting vote.",
                        log_guard.last().term,
                        last_log_term
                    );
                } else if let Some(voted_for) = election_guard.voted_for.as_ref() {
                    info!(
                        target: "raft-rpc",
                        "Already voted for {}; NOT granting vote.",
                        voted_for
                    );
                } else if last_log_term == log_guard.last().term && last_log_index < log_guard.len() {
                    info!(
                        target: "raft-rpc",
                        "Our logs are both at term {}, but our log is {} and theirs is only {} long; NOT granting vote.",
                        log_guard.last().term,
                        log_guard.len(),
                        last_log_index
                    );
                } else {
                    info!(
                        target: "raft-rpc",
                        "Granting vote to {}",
                        candidate_id
                    );
                    grant = true;
                    election_guard.voted_for = Some(candidate_id);
                    election_guard.reset_election_deadline();
                }

                envelope
                    .reply(Message::RequestVoteOk {
                        term: election_guard.term,
                        vote_granted: grant,
                    })
                    .send()
                    .unwrap();
            }
            Message::RequestVoteOk { term, vote_granted } => {
                info!(target: "raft-rpc", "Received: {}", envelope);

                let total_nodes = cluster_guard.all_nodes.len();

                election_guard.reset_step_down_deadline();
                election_guard.maybe_step_down(term).unwrap();

                if election_guard.kind == ElectionMember::Candidate
                    && election_guard.term == term
                    && election_guard.term == election_guard.vote_requested_for_term
                    && vote_granted
                {
                    election_guard.votes.insert(envelope.source.clone());
                    info!(target: "election", "Have votes: {:?}", election_guard.votes);
                }

                if election_guard.votes.len() >= ElectionState::majority(total_nodes)
                    && election_guard.kind == ElectionMember::Candidate
                {
                    election_guard.become_leader().unwrap();
                    {
                        leader_state_guard.last_replication_at = Utc::now();

                        leader_state_guard.next_index.clear();
                        leader_state_guard.match_index.clear();
                        let next_index = log_guard.len() + 1;

                        leader_state_guard.init_own_match_index(cluster_guard.id.clone(), next_index);

                        cluster_guard
                        .other_nodes()
                        .for_each(|node| {
                            leader_state_guard
                            .next_index
                            .entry(node.to_string())
                            .and_modify(|v| *v = next_index)
                            .or_insert(next_index);

                            leader_state_guard
                            .match_index
                            .entry(node.to_string())
                            .and_modify(|v| *v = 0)
                            .or_insert(0);
                        });
                    }
                }
            },

            Message::AppendEntriesOk { term, success , previous_log_index, entries_size } => {
                info!(target: "raft-rpc", "Received: {}", envelope);
                election_guard.maybe_step_down(term).unwrap();

                let ni = previous_log_index + 1;

                if election_guard.is_leader() && election_guard.term == term {
                    election_guard.reset_step_down_deadline();
                    let sender_id = envelope.source.clone();

                    if success {

                        let next_index = leader_state_guard.next_index.get_mut(&sender_id).unwrap();
                        *next_index = (*next_index).max(ni + entries_size);

                        let match_index = leader_state_guard.match_index.get_mut(&sender_id).unwrap();
                        *match_index = (*match_index).max(ni + entries_size - 1);

                        debug!(target: "append-entries-ok", "Next index: \n{:#?}", leader_state_guard.next_index);
                    }
                    else {
                        let next_index = leader_state_guard.next_index.get_mut(&sender_id).unwrap();
                        *next_index -= 1;
                    }
                }
            },
            Message::AppendEntries { 
                term,
                leader_id,
                previous_log_index,
                previous_log_term,
                entries,
                leader_commit_index
            } => {
                info!(target: "append-entries", "Received: {}", envelope);

                election_guard.maybe_step_down(term).unwrap();

                let reply_msg = Message::AppendEntriesOk {
                    term: election_guard.term,
                    success: false,
                    entries_size: entries.len(),
                    previous_log_index
                };
                
                let reply = 
                    envelope
                    .reply(reply_msg);

                if term < election_guard.term {
                    reply.send().unwrap();
                    debug!(target: "append-entries", "Sending: {}", reply);
                    return;
                }

                election_guard.reset_election_deadline();
                if previous_log_index <= 0 {
                    envelope.reply(
                        Message::Error { 
                            code: MaelstromError::Abort as usize, 
                            text: Some(format!("Out of bounds previous log index {}", previous_log_index))
                        }
                    )
                    .send()
                    .unwrap();
                    debug!(target: "append-entries", "Out of bounds previous log index.");
                    return;
                }

                let log_len = log_guard.len();

                match log_guard.get(previous_log_index) {
                    Some(previous_log) if previous_log.term == previous_log_term => {
                        log_guard.truncate(previous_log_index);
                        log_guard.append(&entries);
                    },
                    _ => {
                        reply.send().unwrap();
                        debug!(target: "append-entries", "Sending: {}", reply);
                        return;
                    }
                }

                {
                    if leader_state_guard.commit_index < leader_commit_index {
                        leader_state_guard.commit_index = log_len.min(leader_commit_index);
                    }
                }

                let reply_msg = Message::AppendEntriesOk {
                    term: election_guard.term,
                    success: true,
                    entries_size: entries.len(),
                    previous_log_index
                };
                
                let reply = 
                    envelope
                    .reply(reply_msg);
                
                reply.send().unwrap();
                debug!(target: "append-entries", "Sending: {}", reply);
                return;
            },
            _ => {}
        }
    }

    /// Try to replicate our local log to followers, if we are a leader
    /// and return whether we were able to issue AppendEntries to any peer
    /// at all.
    pub fn replicate_log(&self, force: bool) -> bool {

        let election_guard = self.election.lock().unwrap();
        let leader_state_guard = self.leader_state.lock().unwrap();
        let log_guard = self.log.lock().unwrap();
        let cluster_guard = self.cluster.lock().unwrap();

        let elapsed_time = Utc::now() - leader_state_guard.last_replication_at;
        let min_replication_interval = leader_state_guard.minimum_replication_interval;

        let mut replicated = false;

        if election_guard.is_leader() && elapsed_time >= min_replication_interval {
            cluster_guard
            .other_nodes()
            .for_each(|node| {
                let next_index_for_node = *leader_state_guard.next_index.get(node).unwrap() - 1;
                let entries = log_guard.entries_from_index(next_index_for_node);
                let Some(entries) = entries else {
                    return;
                };
                
                if !entries.is_empty() || elapsed_time > leader_state_guard.heartbeat_interval {
                    debug!(target: "append-entries", "Replicating {}.. to #{}", next_index_for_node, node);

                    EnvelopeBuilder::new()
                    .source(&cluster_guard.id)
                    .destination(node)
                    .message(Message::AppendEntries 
                        { 
                            term: election_guard.term, 
                            leader_id: cluster_guard.id.clone(),
                            previous_log_index: next_index_for_node,
                            previous_log_term: log_guard.get(next_index_for_node).map(|x| x.term).unwrap_or_default(),
                            entries: entries.to_vec(),
                            leader_commit_index: leader_state_guard.commit_index
                        }
                    )
                    .build()
                    .unwrap()
                    .send()
                    .unwrap();
                    replicated = true;
                }
            });
        }
        replicated
    }

}

#[derive(Debug, Error)]
pub enum RaftError {
    #[error("Current term is {0} but asked to advance to {1} which is backwards and terms are monotonically increasing.")]
    TermCannotGoBackwards(usize, usize),
    #[error("Only a candidate can become a leader")]
    MustBeACandidateToBecomeALeader,
    #[error(transparent)]
    Maelstrom(#[from] EnvelopeBuilderError),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ElectionMember {
    #[default]
    Follower,
    Leader,
    Candidate,
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
    pub voted_for: Option<String>,
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
            voted_for: None,
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

    pub fn is_leader(&self) ->  bool {
        self.kind == ElectionMember::Leader
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
            return Err(RaftError::TermCannotGoBackwards(self.term, term));
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
        self.deadline = Utc::now()
            .checked_add_signed(chrono::Duration::from_std(delay_by).unwrap())
            .unwrap();
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
        last_log_term: usize,
    ) -> Result<(), RaftError> {
        let term = {
            self.vote_requested_for_term = self.term;
            self.term
        };
        info!(target: "election", "About to request for votes to our peers ({:?})", other_nodes);
        // Build the envelopes first. Only if all succeed should we send them all out.
        let envelopes = other_nodes
            .iter()
            .map(|neighbor| {
                EnvelopeBuilder::default()
                    .destination(neighbor)
                    .source(our_id)
                    .message(Message::RequestVote {
                        term,
                        candidate_id: our_id.to_string(),
                        last_log_index,
                        last_log_term,
                    })
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
            false => (n - 1) / 2 + 1,
        }
    }
}

/// State stored by leader nodes.
#[derive(Debug, Clone)]
pub struct LeaderState {
    /// The highest committed entry in the log.
    pub commit_index: usize,
    /// A map of nodes to the next index to replicate.
    pub next_index: HashMap<NodeId, usize>,
    /// A map of (other) nodes to the highest log
    /// entry known to be replicated on that node.
    pub match_index: HashMap<NodeId, usize>,
    pub last_replication_at: DateTime<Utc>,
    pub minimum_replication_interval: chrono::Duration,
    pub heartbeat_interval: chrono::Duration
}

impl Default for LeaderState {
    fn default() -> Self {
        Self {
            commit_index: Default::default(),
            next_index: Default::default(),
            match_index: Default::default(),
            last_replication_at: Utc::now(),
            minimum_replication_interval: chrono::Duration::milliseconds(50),
            heartbeat_interval: chrono::Duration::seconds(1),
        }
    }
}

impl LeaderState {
    pub fn reset(&mut self) {
        self.match_index = Default::default();
        self.next_index = Default::default();
    }

    pub fn init_own_match_index(
        &mut self, 
        node_id: NodeId,
        log_size: usize,
    ) {
        self.match_index.insert(node_id, log_size);
    }
}

pub type Shared<T> = Arc<Mutex<T>>;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Message>,
}

impl LogEntry {
    pub fn new(term: usize, data: Option<Envelope<Message>>) -> Self {
        Self { term, data: data.map(|envelope| envelope.message()) }
    }
}

#[derive(Clone, Default)]
pub struct Log {
    entries: Vec<LogEntry>,
}

impl core::fmt::Debug for Log {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f
        .debug_struct("Log")
        .field("first", &self.entries.first())
        .field("last", self.last())
        .field("size", &self.len())
        .finish()
    }
}

impl Log {
    pub fn new() -> Self {
        Self { entries: vec![] }
    }
    pub fn init() -> Self {
        Self {
            entries: vec![LogEntry::new(0, None)],
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
    pub fn last_mut(&mut self) -> &mut LogEntry {
        let last_index = self.entries.len() - 1;
        self.entries.get_mut(last_index).unwrap()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn truncate(&mut self, index: usize) {
        self.entries = self.entries.split_off(index);
    }

    pub fn entries_from_index(&self, i: usize) -> Option<&[LogEntry]> {
        if i <= 0 {
            return None;
        }
        Some(&self.entries[i-1..])
    }

}

pub type NodeId = String;

#[derive(Debug, Clone, Default)]
pub struct ClusterMembership {
    pub id: NodeId,
    pub all_nodes: HashSet<NodeId>,
    pub neighbor_map: HashMap<NodeId, HashSet<NodeId>>,
}

impl ClusterMembership {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn other_nodes(&self) -> impl Iterator<Item = &String> {
        self.all_nodes.iter().filter(|&node| node != &self.id)
    }
}
#[derive(Debug)]
pub struct Node {
    pub state_machine: Shared<RaftRuntime>,
    pub msg_rx: Receiver<Envelope<Message>>,
    pub election: Shared<ElectionState>,
    pub log: Shared<Log>,
    pub cluster_membership: Shared<ClusterMembership>,
    pub leader_state: Shared<LeaderState>
}

impl Node {
    pub fn new(rx: Receiver<Envelope<Message>>) -> Self {
        let cluster_membership: Shared<ClusterMembership> = Default::default();
        let election = Arc::new(Mutex::new(ElectionState::new()));
        let log = Arc::new(Mutex::new(Log::init()));
        let leader_state = Arc::new(Mutex::new(LeaderState::default()));

        Self {
            state_machine: Arc::new(Mutex::new(RaftRuntime::new(
                cluster_membership.clone(),
                election.clone(),
                log.clone(),
                leader_state.clone(),
            ))),
            msg_rx: rx,
            log,
            election,
            cluster_membership,
            leader_state
        }
    }

    pub fn run(&mut self) {

        let election = self.election.clone();
        let log = self.log.clone();
        let cluster_membership = self.cluster_membership.clone();
        let leader_state = self.leader_state.clone();

        let _election_handle = 
            std::thread::Builder::new()
            .name("election".to_string())
            .spawn(move || loop {
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
                            (
                                cluster.id.clone(),
                                cluster.other_nodes().cloned().collect::<Vec<_>>(),
                            )
                        };
                        guard
                            .request_votes(&our_id, &other_nodes, last_log_index, last_log_term)
                            .unwrap();
                        // Vote for self when becoming a candidate.
                        guard.voted_for = Some(our_id.to_string());
                        guard.votes.insert(our_id.clone());

                        let total_nodes = {
                            cluster_membership.lock().unwrap().all_nodes.len()
                        };

                        if guard.votes.len() >= ElectionState::majority(total_nodes)
                            && guard.kind == ElectionMember::Candidate
                        {
                            guard.become_leader().unwrap();
                            {
                                let mut leader_state = leader_state.lock().unwrap();
                                leader_state.last_replication_at = Utc::now();

                                leader_state.next_index.clear();
                                leader_state.match_index.clear();
                                let next_index = log.lock().unwrap().len() + 1;

                                let node_metadata = cluster_membership.lock().unwrap();
                                leader_state.init_own_match_index(node_metadata.id.clone(), next_index);

                                node_metadata
                                .other_nodes()
                                .for_each(|node| {
                                    leader_state
                                    .next_index
                                    .entry(node.to_string())
                                    .and_modify(|v| *v = next_index)
                                    .or_insert(next_index);

                                    leader_state
                                    .match_index
                                    .entry(node.to_string())
                                    .and_modify(|v| *v = 0)
                                    .or_insert(0);
                                });
                            }
                        }
                    } else {
                        guard.reset_election_deadline();
                    }
                }
                drop(guard);

                let mut rng = rand::thread_rng();
                let extra_sleep = Duration::from_millis(rng.gen_range(0..10));
                sleep(Duration::from_millis(100) + extra_sleep);
            }
        );

        let election = self.election.clone();
        let leader_state = self.leader_state.clone();

        let _leader_stepdown_handle = 
            std::thread::Builder::new()
            .name("leader-stepdown".to_string())
            .spawn(move || loop {
                sleep(Duration::from_millis(100));
                let mut guard = election.lock().unwrap();
                if guard.kind == ElectionMember::Leader && guard.step_down_deadline < Utc::now() {
                    info!(target: "election", "Stepping down: haven't received any acks recently.");
                    {
                        let mut guard = leader_state.lock().unwrap();
                        guard.reset();
                    }
                    guard.become_follower();
                }
                drop(guard);
            }
        ).unwrap();

        let state = self.state_machine.clone();
        let min_replication_interval = {
            self.leader_state.lock().unwrap().minimum_replication_interval
        };
        let leader_state = self.leader_state.clone();

        let _replication_handle =
            std::thread::Builder::new()
            .name("replication".to_owned())
            .spawn(move || loop {
                sleep(min_replication_interval.to_std().unwrap());
                {
                    let state_guard = state.lock().unwrap();
                    if state_guard.replicate_log(false) {
                        debug!(target: "replication-thread", "replication successful. updating last_replication at");
                        {
                            leader_state.lock().unwrap().last_replication_at = Utc::now();
                        }
                        debug!(target: "replication-thread", "updated last_replication_at.");
                    }
                }
            })
            .unwrap();

        let log = self.log.clone();
        let _log_logger_handle = 
            std::thread::Builder::new()
            .name("log-logger".to_string())
            .spawn(move || loop {
                sleep(Duration::from_millis(200));
                {
                    debug!(target: "log-logger", "Current log: \n{:#?}", &*log.lock().unwrap());
                }
            })
            .unwrap();

        while let Ok(envelope) = self.msg_rx.recv() {
            let mut guard = self.state_machine.lock().unwrap();
            guard.handle(&envelope);
            drop(guard);
        }


    }
}

pub fn main() {
    let logger = simple_logger::SimpleLogger::new();
    logger
        .with_level(log::LevelFilter::Info)
        .env()
        .init()
        .unwrap();

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
