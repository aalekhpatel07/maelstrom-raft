use serde::{Serialize, Deserialize};
use thiserror::Error;
use std::{
    io::Write,
    sync::atomic::{AtomicUsize, Ordering}
};
use core::result::Result;



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
    #[serde(rename="src")]
    pub source: String,
    #[serde(rename="dest")]
    pub destination: String,

    pub(crate) body: Body<Message>
}

impl<Message> Envelope<Message> 
where
    Message: Serialize
{
    pub fn message(&self) -> &Message {
        self.body.message()
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
        let mut stdout = std::io::stdout().lock();
        serde_json::to_writer(&mut stdout, self)?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;

        Ok(())
    }
}


#[derive(Debug)]
pub struct EnvelopeBuilder<Message> {
    source: Option<String>,
    destination: Option<String>,
    msg_id: Option<usize>,
    in_reply_to: Option<usize>,
    message: Option<Message>
}

impl<Message> Default for EnvelopeBuilder<Message> {
    fn default() -> Self {
        Self {
            source: None,
            destination: None,
            msg_id: Some(MESSAGE_ID.fetch_add(1, Ordering::SeqCst)),
            in_reply_to: None,
            message: None
        }
    }
}


#[derive(Error, Debug)]
pub enum EnvelopeIOError {
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    IO(#[from] std::io::Error)
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum EnvelopeBuilderError {
    #[error("Field `{0}` is not configured on the builder but is required.")]
    MissingField(String)
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
        Ok(
            Envelope { 
                source, 
                destination, 
                body: Body { msg_id: self.msg_id, in_reply_to: self.in_reply_to, message }
            }
        )
    }
}