use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

/// Streams command output while it runs, see [CommandOptions::live][crate::CommandOptions::live].
pub type LiveLogSender = mpsc::UnboundedSender<LiveLogEvent>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LiveLogEvent {
  /// A new command started, output after this belongs to it.
  Start {
    stage: String,
    command: String,
  },
  Stdout(String),
  Stderr(String),
}

impl LiveLogEvent {
  pub fn sanitize(self, replacers: &[(String, String)]) -> Self {
    let replace = |s: String| svi::replace_in_string(&s, replacers);
    match self {
      LiveLogEvent::Start { stage, command } => LiveLogEvent::Start {
        stage,
        command: replace(command),
      },
      LiveLogEvent::Stdout(line) => {
        LiveLogEvent::Stdout(replace(line))
      }
      LiveLogEvent::Stderr(line) => {
        LiveLogEvent::Stderr(replace(line))
      }
    }
  }
}
