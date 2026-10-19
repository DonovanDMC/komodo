use std::{sync::Arc, time::Duration};

use anyhow::Context;
use command::LiveLogEvent;
use komodo_client::entities::{
  komodo_timestamp,
  update::{Log, Update},
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
  connection::TerminalChannels, helpers::update::update_update,
  periphery::PeripheryClient, state::periphery_connections,
};

/// How often the update is saved while output is coming in.
const FLUSH_INTERVAL: Duration = Duration::from_secs(1);

/// Collects command output streamed from Periphery
/// into the update while the request runs.
pub struct LiveLog {
  pub channel: Uuid,
  cancel: CancellationToken,
  handle: JoinHandle<()>,
  terminals: Arc<TerminalChannels>,
}

impl PeripheryClient {
  /// Output is appended after the logs already on `update`.
  /// Pass the [LiveLog::channel] with the request, then call
  /// [LiveLog::finish] once it returns.
  pub async fn live_log(
    &self,
    update: &Update,
  ) -> anyhow::Result<LiveLog> {
    let connection =
      periphery_connections().get(&self.id).await.with_context(
        || format!("No connection found for server {}", self.id),
      )?;

    let channel = Uuid::new_v4();
    let (sender, mut receiver) = transport::channel::channel();
    connection.terminals.insert(channel, sender).await;

    let cancel = CancellationToken::new();
    let update = update.clone();
    let handle = tokio::spawn({
      let cancel = cancel.clone();
      async move {
        let mut logs = Vec::<Log>::new();
        let mut dirty = false;
        let mut interval = tokio::time::interval(FLUSH_INTERVAL);
        loop {
          tokio::select! {
            _ = cancel.cancelled() => break,
            message = receiver.recv() => {
              let Ok(Ok(bytes)) = message else { break };
              match serde_json::from_slice(&bytes) {
                Ok(event) => {
                  apply_event(&mut logs, event);
                  dirty = true;
                }
                Err(e) => {
                  warn!("Failed to parse live log event | {e:#}")
                }
              }
            }
            _ = interval.tick() => {
              if !dirty {
                continue;
              }
              dirty = false;
              let mut update = update.clone();
              update.logs.extend(logs.iter().cloned());
              if let Err(e) = update_update(update).await {
                warn!("Failed to save live log | {e:#}");
              }
            }
          }
        }
      }
    });

    Ok(LiveLog {
      channel,
      cancel,
      handle,
      terminals: connection.terminals.clone(),
    })
  }
}

impl LiveLog {
  /// Stops collecting. The caller then sets the final logs,
  /// replacing the live ones.
  pub async fn finish(self) {
    self.cancel.cancel();
    let _ = self.handle.await;
    self.terminals.remove(&self.channel).await;
  }
}

fn apply_event(logs: &mut Vec<Log>, event: LiveLogEvent) {
  match event {
    LiveLogEvent::Start { stage, command } => {
      let ts = komodo_timestamp();
      logs.push(Log {
        stage,
        command,
        success: true,
        start_ts: ts,
        end_ts: ts,
        ..Default::default()
      });
    }
    LiveLogEvent::Stdout(line) => {
      if let Some(log) = logs.last_mut() {
        log.stdout.push_str(&line);
      }
    }
    LiveLogEvent::Stderr(line) => {
      if let Some(log) = logs.last_mut() {
        log.stderr.push_str(&line);
      }
    }
  }
}
