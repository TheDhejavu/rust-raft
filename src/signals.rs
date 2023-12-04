use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::signal;
use log::info;

use crate::error;

#[derive(Debug, PartialEq)]
pub enum ServerEventCommand {
    Shutdown,
    StepDown,
}

/// Types of process signals.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SignalKind {
    Int,
    Term,
    Quit,
}

impl fmt::Display for SignalKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SignalKind::Int => "SIGINT",
            SignalKind::Term => "SIGTERM",
            SignalKind::Quit => "SIGQUIT",
        })
    }
}

/// Process signal listener.
pub(crate) struct Signals {
    #[cfg(unix)]
    signals: Vec<(SignalKind, signal::unix::Signal)>,
}

impl Signals {
    /// Constructs an OS signal listening future.
    pub(crate) fn new() -> Self {
        info!("Setting up OS signal listener");

        #[cfg(unix)]
        let signals = {
            let sig_map = [
                (signal::unix::SignalKind::interrupt(), SignalKind::Int),
                (signal::unix::SignalKind::terminate(), SignalKind::Term),
                (signal::unix::SignalKind::quit(), SignalKind::Quit),
            ];

            let signals = sig_map
                .iter()
                .filter_map(|(kind, sig)| {
                    signal::unix::signal(*kind)
                        .map(|tokio_sig| (*sig, tokio_sig))
                        .map_err(|e| {
                            error!(
                                "Can not initialize stream handler for {:?} err: {}",
                                sig,
                                e
                            )
                        })
                        .ok()
                })
                .collect::<Vec<_>>();

            Signals { signals }
        };

        #[cfg(not(unix))]
        let signals = Signals { signals: Vec::new() };

        signals
    }

    pub fn map_signal(signal: SignalKind) -> ServerEventCommand {
        match signal {
            SignalKind::Int | SignalKind::Term | SignalKind::Quit => {
                info!("{:?} received; starting graceful shutdown", signal);
                ServerEventCommand::Shutdown
            }
        }
    }
    
}

impl Future for Signals {
    type Output = SignalKind;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        #[cfg(unix)]
        {
            for (sig, fut) in self.signals.iter_mut() {
                if fut.poll_recv(cx).is_ready() {
                    info!("{} received", sig);
                    return Poll::Ready(*sig);
                }
            }

            Poll::Pending
        }

        // Non-Unix systems don't support signals, so simulate a sleep
        #[cfg(not(unix))]
        {
            // Sleep for a short duration to simulate waiting for a signal
            tokio::time::sleep(Duration::from_millis(100)).await;
            Poll::Pending
        }
    }
}


