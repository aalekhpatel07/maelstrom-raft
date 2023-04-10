use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::sync::mpsc::Receiver;

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
    }
}


#[derive(Debug, Clone, Default)]
pub struct State {
    state: HashMap<usize, usize>
}

impl State {
    pub fn handle(&mut self, envelope: &Envelope<Message>) {
        match envelope.message() {
            Message::Init { .. } => {
                envelope.reply(Message::InitOk).send().unwrap();
            },
            Message::Topology { .. } => {
                envelope.reply(Message::TopologyOk).send().unwrap();
            },
            Message::Read { key } => {
                if !self.state.contains_key(&key) {
                    let error_text = format!("Key {} not found.", key);
                    envelope.reply(Message::Error { code: 20, text: Some(error_text)}).send().unwrap();
                    return;
                }
                let value = *self.state.get(&key).unwrap();
                envelope.reply(Message::ReadOk { value }).send().unwrap();
            },
            Message::Write { key, value } => {
                self.state
                .entry(key)
                .and_modify(|v| *v = value.clone())
                .or_insert(value);
                envelope.reply(Message::WriteOk).send().unwrap();
            },
            Message::Cas { key, from, to } => {

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
            _ => {
                
            }
        }
    }
}


#[derive(Debug)]
pub struct Node {
    pub state_machine: State,
    pub msg_rx: Receiver<Envelope<Message>>
}

impl Node {
    pub fn new(rx: Receiver<Envelope<Message>>) -> Self {
        Self {
            state_machine: State::default(),
            msg_rx: rx
        }
    }
    pub fn run(mut self) {
        while let Ok(envelope) = self.msg_rx.recv() {
            self.state_machine.handle(&envelope);
        }
    }
}

pub fn main() {
    let (stdin_tx, stdin_rx) = std::sync::mpsc::channel();

    std::thread::spawn(move || {
        for input in std::io::stdin().lines() {
            let line = input.unwrap();
            let deserialized: Envelope<Message> = serde_json::from_str(&line).unwrap();
            stdin_tx.send(deserialized).unwrap();
        }
    });

    let node = Node::new(stdin_rx);
    node.run();
}