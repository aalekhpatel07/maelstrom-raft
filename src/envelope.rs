use core::result::Result;
use log::trace;
use serde::{Deserialize, Serialize};
use std::{
    io::Write,
    sync::atomic::{AtomicUsize, Ordering},
};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<Message> {
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<usize>,

    #[serde(flatten)]
    message: Message,
}

impl<Message> Body<Message> {
    pub fn msg_id(&self) -> Option<usize> {
        self.msg_id
    }
    pub fn in_reply_to(&self) -> Option<usize> {
        self.in_reply_to
    }
    pub fn message(&self) -> &Message {
        &self.message
    }
}

pub static MESSAGE_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Envelope<Message> {
    #[serde(rename = "src")]
    pub source: String,
    #[serde(rename = "dest")]
    pub destination: String,

    pub(crate) body: Body<Message>,
}

impl<Message> std::fmt::Display for Envelope<Message>
where
    Message: Serialize,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string_pretty(self).unwrap())
    }
}

impl<Message> Envelope<Message>
where
    Message: Serialize + Clone,
{
    pub fn message(&self) -> Message {
        self.body.message().clone()
    }
    pub fn msg_id(&self) -> Option<usize> {
        self.body.msg_id()
    }

    pub fn is_internal(&self) -> bool {
        self.source.starts_with('n')
    }

    pub fn reply(&self, message: Message) -> Envelope<Message> {
        EnvelopeBuilder::new()
            .source(&self.destination)
            .destination(&self.source)
            .in_reply_to(self.msg_id())
            .message(message)
            .build()
            .unwrap() // Can only ever create replies to legitimate envelopes so this is okay to unwrap.
    }

    pub fn as_json_pretty(&self) -> Result<String, EnvelopeIOError> {
        serde_json::to_string_pretty(self).map_err(EnvelopeIOError::Serde)
    }

    pub fn send(&self) -> Result<(), EnvelopeIOError> {
        let prettified = serde_json::to_string_pretty(self)?;
        let mut stdout = std::io::stdout().lock();
        serde_json::to_writer(&mut stdout, self)?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;

        trace!(target: "envelope", "Sending: {}", prettified);

        Ok(())
    }
}

#[derive(Debug)]
pub struct EnvelopeBuilder<Message> {
    source: Option<String>,
    destination: Option<String>,
    msg_id: Option<usize>,
    in_reply_to: Option<usize>,
    message: Option<Message>,
}

impl<Message> Default for EnvelopeBuilder<Message> {
    fn default() -> Self {
        Self {
            source: None,
            destination: None,
            msg_id: Some(MESSAGE_ID.fetch_add(1, Ordering::SeqCst)),
            in_reply_to: None,
            message: None,
        }
    }
}

#[derive(Error, Debug)]
pub enum EnvelopeIOError {
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    IO(#[from] std::io::Error),
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum EnvelopeBuilderError {
    #[error("Field `{0}` is not configured on the builder but is required.")]
    MissingField(String),
}

impl<Message> EnvelopeBuilder<Message> {
    pub fn new() -> Self {
        Default::default()
    }
    pub fn in_reply_to(self, in_reply_to: Option<usize>) -> Self {
        Self {
            in_reply_to,
            ..self
        }
    }
    pub fn source(self, source: &str) -> Self {
        Self {
            source: Some(source.into()),
            ..self
        }
    }
    pub fn destination(self, destination: &str) -> Self {
        Self {
            destination: Some(destination.into()),
            ..self
        }
    }
    pub fn message(self, message: Message) -> Self {
        Self {
            message: Some(message),
            ..self
        }
    }

    pub fn build(self) -> Result<Envelope<Message>, EnvelopeBuilderError> {
        let Some(source) = self.source else {
            return Err(EnvelopeBuilderError::MissingField("source".into()));
        };
        let Some(destination) = self.destination else {
            return Err(EnvelopeBuilderError::MissingField("destination".into()));
        };
        let Some(message) = self.message else {
            return Err(EnvelopeBuilderError::MissingField("message".into()));
        };
        Ok(Envelope {
            source,
            destination,
            body: Body {
                msg_id: self.msg_id,
                in_reply_to: self.in_reply_to,
                message,
            },
        })
    }
}


#[derive(Debug, Clone, Copy)]
pub enum MaelstromError {
    /// Indicates that the requested operation could not be completed within a timeout.
    Timeout = 0,
    /// Thrown when a client sends an RPC request to a node which does not exist.
    NodeNotFound = 1,
    /// Use this error to indicate that a requested operation is 
    /// not supported by the current implementation. 
    /// Helpful for stubbing out APIs during development.
    NotSupported = 10,
    /// Indicates that the operation definitely cannot be performed 
    /// at this time--perhaps because the server is in a read-only state, 
    /// has not yet been initialized, believes its peers to be down, 
    /// and so on. Do not use this error for indeterminate cases, 
    /// when the operation may actually have taken place.
    TemporarilyUnavailable = 11,
    /// The client's request did not conform to the server's expectations, 
    /// and could not possibly have been processed.
    MalformedRequest = 12,
    /// Indicates that some kind of general, indefinite error occurred. 
    /// Use this as a catch-all for errors you can't otherwise categorize, 
    /// or as a starting point for your error handler: it's safe to 
    /// return `internal-error` for every problem by default, 
    /// then add special cases for more specific errors later.
    Crash = 13,
    /// Indicates that some kind of general, definite error occurred. 
    /// Use this as a catch-all for errors you can't otherwise categorize, 
    /// when you specifically know that the requested operation has not 
    /// taken place. For instance, you might encounter an indefinite failure 
    /// during the prepare phase of a transaction: since you haven't started 
    /// the commit process yet, the transaction can't have taken place. 
    /// It's therefore safe to return a definite abort to the client.
    Abort = 14,
    /// The client requested an operation on a key which does not exist 
    /// (assuming the operation should not automatically create missing keys).
    KeyDoesNotExist = 20,
    /// The client requested the creation of a key which already exists, 
    /// and the server will not overwrite it.
    KeyAlreadyExists = 21,
    /// The requested operation expected some conditions to hold, 
    /// and those conditions were not met. For instance, a 
    /// compare-and-set operation might assert that the value 
    /// of a key is currently 5; if the value is 3, the server 
    /// would return `precondition-failed`.
    PreconditionFailed = 22,
    /// The requested transaction has been aborted because of a 
    /// conflict with another transaction. Servers need not return 
    /// this error on every conflict: they may choose to retry 
    /// automatically instead.
    TxnConflict = 30
}