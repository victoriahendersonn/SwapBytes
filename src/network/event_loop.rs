use futures::{channel::{mpsc, oneshot}, SinkExt, StreamExt};
use libp2p::{
    kad,
    multiaddr::Protocol,
    request_response::{self, OutboundRequestId},
    swarm::{Swarm, SwarmEvent},
    PeerId,
};
use std::collections::{hash_map, HashMap, HashSet};
use std::error::Error;

use crate::network::behaviour::{ChatBehaviour, ChatBehaviourEvent};
use crate::network::command::Command;
use crate::network::event::Event;
use crate::network::behaviour::FileRequest;
use crate::network::behaviour::FileResponse;

pub struct EventLoop {
    swarm: Swarm<ChatBehaviour>,
    command_receiver: mpsc::Receiver<Command>,
    event_sender: mpsc::Sender<Event>,
    pending_dial: HashMap<PeerId, oneshot::Sender<Result<(), Box<dyn Error + Send>>>>,
    pending_start_providing: HashMap<kad::QueryId, oneshot::Sender<()>>,
    pending_get_providers: HashMap<kad::QueryId, oneshot::Sender<HashSet<PeerId>>>,
    pending_request_file:
        HashMap<OutboundRequestId, oneshot::Sender<Result<Vec<u8>, Box<dyn Error + Send>>>>,
}

impl EventLoop {
    pub fn new(
        swarm: Swarm<ChatBehaviour>,
        command_receiver: mpsc::Receiver<Command>,
        event_sender: mpsc::Sender<Event>,
    ) -> Self {
        Self {
            swarm,
            command_receiver,
            event_sender,
            pending_dial: Default::default(),
            pending_start_providing: Default::default(),
            pending_get_providers: Default::default(),
            pending_request_file: Default::default(),
        }
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => self.handle_event(event).await,
                command = self.command_receiver.next() => match command {
                    Some(c) => self.handle_command(c).await,
                    // Command channel closed, thus shutting down the network event loop.
                    None=>  return,
                },
            }
        }
    }

    pub async fn handle_event(&mut self, event: SwarmEvent<ChatBehaviourEvent>) {
        match event {
            SwarmEvent::Behaviour(ChatBehaviourEvent::Kademlia(
                kad::Event::OutboundQueryProgressed {
                    id,
                    result: kad::QueryResult::StartProviding(_),
                    ..
                },
            )) => {
                let sender: oneshot::Sender<()> = self
                    .pending_start_providing
                    .remove(&id)
                    .expect("Completed query to be previously pending.");
                let _ = sender.send(());
            }
            SwarmEvent::Behaviour(ChatBehaviourEvent::Kademlia(
                kad::Event::OutboundQueryProgressed {
                    id,
                    result:
                        kad::QueryResult::GetProviders(Ok(kad::GetProvidersOk::FoundProviders {
                            providers,
                            ..
                        })),
                    ..
                },
            )) => {
                if let Some(sender) = self.pending_get_providers.remove(&id) {
                    sender.send(providers).expect("Receiver not to be dropped");

                    // Finish the query. We are only interested in the first result.
                    self.swarm
                        .behaviour_mut()
                        .kademlia
                        .query_mut(&id)
                        .unwrap()
                        .finish();
                }
            }
            SwarmEvent::Behaviour(ChatBehaviourEvent::Kademlia(
                kad::Event::OutboundQueryProgressed {
                    result:
                        kad::QueryResult::GetProviders(Ok(
                            kad::GetProvidersOk::FinishedWithNoAdditionalRecord { .. },
                        )),
                    ..
                },
            )) => {}
            SwarmEvent::Behaviour(ChatBehaviourEvent::Kademlia(_)) => {}
            SwarmEvent::Behaviour(ChatBehaviourEvent::RequestResponse(
                request_response::Event::Message { message, .. },
            )) => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    self.event_sender
                        .send(Event::InboundRequest {
                            request: request.0,
                            channel,
                        })
                        .await
                        .expect("Event receiver not to be dropped.");
                }
                request_response::Message::Response {
                    request_id,
                    response,
                } => {
                    let _ = self
                        .pending_request_file
                        .remove(&request_id)
                        .expect("Request to still be pending.")
                        .send(Ok(response.0));
                }
            },
            SwarmEvent::Behaviour(ChatBehaviourEvent::RequestResponse(
                request_response::Event::OutboundFailure {
                    request_id, error, ..
                },
            )) => {
                let _ = self
                    .pending_request_file
                    .remove(&request_id)
                    .expect("Request to still be pending.")
                    .send(Err(Box::new(error)));
            }
            SwarmEvent::Behaviour(ChatBehaviourEvent::RequestResponse(
                request_response::Event::ResponseSent { .. },
            )) => {}
            SwarmEvent::NewListenAddr { address, .. } => {
                let local_peer_id = *self.swarm.local_peer_id();
                eprintln!(
                    "Local node is listening on {:?}",
                    address.with(Protocol::P2p(local_peer_id))
                );
            }
            SwarmEvent::IncomingConnection { .. } => {}
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => {
                if endpoint.is_dialer() {
                    if let Some(sender) = self.pending_dial.remove(&peer_id) {
                        let _ = sender.send(Ok(()));
                    }
                }
            }
            SwarmEvent::ConnectionClosed { .. } => {}
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                if let Some(peer_id) = peer_id {
                    if let Some(sender) = self.pending_dial.remove(&peer_id) {
                        let _ = sender.send(Err(Box::new(error)));
                    }
                }
            }
            SwarmEvent::IncomingConnectionError { .. } => {}
            SwarmEvent::Dialing {
                peer_id: Some(peer_id),
                ..
            } => eprintln!("Dialing {peer_id}"),
            e => panic!("{e:?}"),
        }
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::StartListening { addr, sender } => {
                let _ = match self.swarm.listen_on(addr) {
                    Ok(_) => sender.send(Ok(())),
                    Err(e) => sender.send(Err(Box::new(e))),
                };
            }
            Command::Dial {
                peer_id,
                peer_addr,
                sender,
            } => {
                if let hash_map::Entry::Vacant(e) = self.pending_dial.entry(peer_id) {
                    self.swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&peer_id, peer_addr.clone());
                    match self.swarm.dial(peer_addr.with(Protocol::P2p(peer_id))) {
                        Ok(()) => {
                            e.insert(sender);
                        }
                        Err(e) => {
                            let _ = sender.send(Err(Box::new(e)));
                        }
                    }
                } else {
                    todo!("Already dialing peer.");
                }
            }
            Command::StartProviding { file_name, sender } => {
                let query_id = self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .start_providing(file_name.into_bytes().into())
                    .expect("No store error.");
                self.pending_start_providing.insert(query_id, sender);
            }
            Command::GetProviders { file_name, sender } => {
                let query_id = self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .get_providers(file_name.into_bytes().into());
                self.pending_get_providers.insert(query_id, sender);
            }
            Command::RequestFile {
                file_name,
                peer,
                sender,
            } => {
                let request_id = self
                    .swarm
                    .behaviour_mut()
                    .request_response
                    .send_request(&peer, FileRequest(file_name));
                self.pending_request_file.insert(request_id, sender);
            }
            Command::ResponseFile { file, channel } => {
                self.swarm
                    .behaviour_mut()
                    .request_response
                    .send_response(channel, FileResponse(file))
                    .expect("Connection to peer to be still open.");
            }
        }
    }
}