use futures::channel::{mpsc, oneshot};
use futures::prelude::*;
use futures::StreamExt;
use tokio::io::{self, AsyncBufReadExt};

use std::sync::{Arc, Mutex, MutexGuard};
use lazy_static::lazy_static;

use libp2p::kad::QueryId;
use libp2p::{
    kad,
	gossipsub, kad::store::MemoryStore, mdns,
    multiaddr::Protocol,
    noise,
    request_response::{self, OutboundRequestId, ProtocolSupport, ResponseChannel},
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    tcp, yamux, PeerId,
};

use libp2p::StreamProtocol;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::time::Duration;


#[derive(Default)]
pub struct GlobalState {
    pub nickname: String,
    pub nicknames: HashMap<String, String>,
    pub queries: HashMap<QueryId, (PeerId, String)>,
    pub peers: Vec<PeerId>,
    pub rooms: Vec<String>,
    pub current_room: String, // state of the current room
}


impl GlobalState {
    pub fn new() -> GlobalState {
		let mut state = GlobalState::default();
		state.current_room = "global-chat".to_string();
		state.rooms.append(&mut vec!["global-chat".to_string()]);
		state
    }
}

// allows the global state mutable and accessible safely across threads
lazy_static! {
    pub static ref STATE: Arc<Mutex<GlobalState>> = Arc::new(Mutex::new(GlobalState::new()));
}

pub fn get_state() -> MutexGuard<'static, GlobalState> {
	STATE.lock().unwrap()
}

const NICKNAME_KEY_PREFIX: &str = "nickname/";

// a macro that is defined by libp2p called network behaviour
#[derive(NetworkBehaviour)]
struct ChatBehaviour {
    // what is chat behaviour event then?
    // chat behaviour event gets added automatically (the compiler makes it!)
    pub mdns: mdns::tokio::Behaviour,
    pub gossipsub: gossipsub::Behaviour,
    pub request_response: request_response::cbor::Behaviour<FileRequest, FileResponse>,
    pub kademlia: kad::Behaviour<MemoryStore>,
}

/// Creates the network components, namely:
///
/// - The network client to interact with the network layer from anywhere
///   within your application.
///
/// - The network event stream, e.g. for incoming requests.
///
/// - The network task driving the network itself.
pub(crate) async fn swarm() -> Result<(Client, impl Stream<Item = Event>, EventLoop), Box<dyn Error>> {
    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(), 
            noise::Config::new,
            yamux::Config::default
        )? // how are we going to send the bits back and forth across our network?
        .with_quic() // will upgrade from TCP to QUIC if it can... 
        .with_behaviour(|key| {
            Ok(ChatBehaviour { 
                mdns: mdns::tokio::Behaviour::new(
                    mdns::Config::default(),
                    key.public().to_peer_id()
                )?,
                gossipsub: gossipsub::Behaviour::new(
                    gossipsub::MessageAuthenticity::Signed(key.clone()),
                    gossipsub::Config::default(),
                )?,
                request_response: request_response::cbor::Behaviour::new(
                    [(
                        StreamProtocol::new("/file-exchange/1"),
                        ProtocolSupport::Full,
                    )],
                    request_response::Config::default(),
                ),
                kademlia: kad::Behaviour::new(
                    key.public().to_peer_id(),
                    MemoryStore::new(key.public().to_peer_id()),
                ),
            })
        })?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();
		// this builds up a swarm. using gossip sub -> a chat room where people can subscribes to topics and if a msg
        // gets sent then all subscribers will receive the msg :) (propagates)

	let mut stdin: io::Lines<io::BufReader<io::Stdin>> = io::BufReader::new(io::stdin()).lines();
    
    println!("Please enter a nickname:");
    let new_nickname = stdin.next_line().await.unwrap().unwrap();

    let mut state = STATE.lock().unwrap();
    state.nickname = new_nickname;

    println!(
        "Welcome to SwapBytes, {}! Please enter chat messages one line at a time.",
        state.nickname
    );

	// gossipsub 
	let topic = gossipsub::IdentTopic::new("global-chat");
	// subscribing to the topic
	swarm.behaviour_mut().gossipsub.subscribe(&topic)?;
	
    // kademlia
    swarm
        .behaviour_mut()
        .kademlia
        .set_mode(Some(kad::Mode::Server));

    // will listen on local host over two different protocols (UDP and TCP)
    // will randomly select a port for TCP 
    //swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    // QUIC is built on top of UDP -> we have to tell it that it has to be through QUIC
    swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
    // ^^ what ports do we want to listen on?
	
    let (command_sender, command_receiver) = mpsc::channel(0);
    let (event_sender, event_receiver) = mpsc::channel(0);

    Ok((
        Client {
            sender: command_sender,
        },
        event_receiver,
        EventLoop::new(swarm, command_receiver, event_sender),
    ))
}

#[derive(Clone)]
pub(crate) struct Client {
    sender: mpsc::Sender<Command>,
}

impl Client {
    /// Listen for incoming connections on the given address.
    // pub(crate) async fn start_listening(
    //     &mut self,
    //     addr: Multiaddr,
    // ) -> Result<(), Box<dyn Error + Send>> {
    //     let (sender, receiver) = oneshot::channel();
    //     self.sender
    //         .send(Command::StartListening { addr, sender })
    //         .await
    //         .expect("Command receiver not to be dropped.");
    //     receiver.await.expect("Sender not to be dropped.")
    // }

    /// Dial the given peer at the given address.
    // pub(crate) async fn dial(
    //     &mut self,
    //     peer_id: PeerId,
    //     peer_addr: Multiaddr,
    // ) -> Result<(), Box<dyn Error + Send>> {
    //     let (sender, receiver) = oneshot::channel();
    //     self.sender
    //         .send(Command::Dial {
    //             peer_id,
    //             peer_addr,
    //             sender,
    //         })
    //         .await
    //         .expect("Command receiver not to be dropped.");
    //     receiver.await.expect("Sender not to be dropped.")
    // }

    /// Advertise the local node as the provider of the given file on the DHT.
    pub(crate) async fn start_providing(&mut self, file_name: String) {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::StartProviding { file_name, sender })
            .await
            .expect("Command receiver not to be dropped.");
        receiver.await.expect("Sender not to be dropped.");
    }

    /// Find the providers for the given file on the DHT.
    pub(crate) async fn get_providers(&mut self, file_name: String) -> HashSet<PeerId> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::GetProviders { file_name, sender })
            .await
            .expect("Command receiver not to be dropped.");
        receiver.await.expect("Sender not to be dropped.")
    }

    /// Request the content of the given file from the given peer.
    pub(crate) async fn request_file(
        &mut self,
        peer: PeerId,
        file_name: String,
    ) -> Result<Vec<u8>, Box<dyn Error + Send>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::RequestFile {
                file_name,
                peer,
                sender,
            })
            .await
            .expect("Command receiver not to be dropped.");
        receiver.await.expect("Sender not be dropped.")
    }

    /// Respond with the provided file content to the given request.
    pub(crate) async fn respond_file(
        &mut self,
        file: Vec<u8>,
        channel: ResponseChannel<FileResponse>,
    ) {
        self.sender
            .send(Command::RespondFile { file, channel })
            .await
            .expect("Command receiver not to be dropped.");
    }

	pub(crate) async fn publish_message(
		&mut self,
		message: String
	) {
		self.sender
			.send(Command::PublishMessage { message })
			.await
			.expect("Publish messages command receiver not to be dropped.");
	}
}

pub(crate) struct EventLoop {
    swarm: Swarm<ChatBehaviour>,
    command_receiver: mpsc::Receiver<Command>,
    event_sender: mpsc::Sender<Event>,
    pending_dial: HashMap<PeerId, oneshot::Sender<Result<(), Box<dyn Error + Send>>>>,
    pending_start_providing: HashMap<kad::QueryId, oneshot::Sender<()>>,
    pending_get_providers: HashMap<kad::QueryId, oneshot::Sender<HashSet<PeerId>>>,
    pending_request_file:
        HashMap<OutboundRequestId, oneshot::Sender<Result<Vec<u8>, Box<dyn Error + Send>>>>,
	queries: HashMap<QueryId, PeerId>,
}

impl EventLoop {
    fn new(
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
			queries: Default::default(),
        }
    }

    pub(crate) async fn run(mut self) {
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

    async fn handle_event(&mut self, event: SwarmEvent<ChatBehaviourEvent>) {
        match event {
				SwarmEvent::NewListenAddr { address, .. } => {
					if address.to_string().contains("/ip4/127.0.0.1/udp") {
						let peer_id = self.swarm.local_peer_id().clone();
						self.swarm.behaviour_mut().kademlia.add_address(&peer_id, address);
						
						let mut state = get_state();
						// Serialize nickname.
						let nickname_bytes = serde_cbor::to_vec(&state.nickname).unwrap();
						let key: String = NICKNAME_KEY_PREFIX.to_string() + &peer_id.to_string();
					
						let record = kad::Record {
							key: kad::RecordKey::new(&key),
							value: nickname_bytes,
							publisher: None,
							expires: None,
						};

						self.swarm.behaviour_mut().kademlia
							.put_record(record, kad::Quorum::One)
							.expect("Failed to locally store record");
					}
			}

			SwarmEvent::Behaviour(ChatBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
				for (peer_id, multiaddr) in list {
					println!("Discovered peer: {} with address: {}", peer_id, multiaddr);
					self.swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                	self.swarm.behaviour_mut().kademlia.add_address(&peer_id, multiaddr);

					// Add peer to local state
					{
						STATE.lock().unwrap().peers.push(peer_id);
					}
					
					// // fetch the nickname from kademlia
					// let key_string = NICKNAME_KEY_PREFIX.to_string() + &peer_id.to_string();
					// let key = kad::RecordKey::new(&key_string);
					// let query_id = self.swarm.behaviour_mut().kademlia.get_record(key);
					// self.queries.insert(query_id, peer_id);
				}
			}

			SwarmEvent::Behaviour(ChatBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
				// removing an expired peer!
				for (peer_id, multiaddr) in list {
					self.swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
					self.swarm.behaviour_mut().kademlia.remove_address(&peer_id, &multiaddr);
				}
			}

			SwarmEvent::Behaviour(ChatBehaviourEvent::Gossipsub(gossipsub::Event::Message {
				propagation_source: peer_id,
				message_id: _id, 
				message,
			})) => {
				let mut state = STATE.lock().unwrap();

				// Message display information
				let topic = message.topic.to_string();
				let data = String::from_utf8_lossy(&message.data).to_string();
				let nickname = state.nicknames.get(&peer_id.to_string()).expect("User not found").clone();      

				println!("{}: {}: {}", nickname, topic, data);
            }
			
            // SwarmEvent::Behaviour(ChatBehaviourEvent::Kademlia(
            //     kad::Event::OutboundQueryProgressed {
            //         id,
            //         result: kad::QueryResult::StartProviding(_),
            //         ..
            //     },
            // )) => {
            //     let sender: oneshot::Sender<()> = self
            //         .pending_start_providing
            //         .remove(&id)
            //         .expect("Completed query to be previously pending.");
            //     let _ = sender.send(());
            // }


            // SwarmEvent::Behaviour(ChatBehaviourEvent::Kademlia(
            //     kad::Event::OutboundQueryProgressed {
            //         id,
            //         result:
            //             kad::QueryResult::GetProviders(Ok(kad::GetProvidersOk::FoundProviders {
            //                 providers,
            //                 ..
            //             })),
            //         ..
            //     },
            // )) => {
            //     if let Some(sender) = self.pending_get_providers.remove(&id) {
            //         sender.send(providers).expect("Receiver not to be dropped");

            //         // Finish the query. We are only interested in the first result.
            //         self.swarm
            //             .behaviour_mut()
            //             .kademlia
            //             .query_mut(&id)
            //             .unwrap()
            //             .finish();
            //     }
            // }

            // SwarmEvent::Behaviour(ChatBehaviourEvent::Kademlia(
            //     kad::Event::OutboundQueryProgressed {
			// 		id,
            //         result:
            //             kad::QueryResult::GetProviders(Ok(
            //                 kad::GetProvidersOk::FinishedWithNoAdditionalRecord { .. },
            //             )),
            //         ..
            //     },
            // )) => {}

			// SwarmEvent::Behaviour(ChatBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
			// 	id, 
			// 	result, 
			// 	..
			// })) => {
			// 	match result {
			// 		// Get record return result
			// 		kad::QueryResult::GetRecord(Ok(
			// 			kad::GetRecordOk::FoundRecord(kad::PeerRecord {
			// 				record: kad::Record { key, value, ..},
			// 				..
			// 			})
			// 		)) => {
			// 			match serde_cbor::from_slice::<String>(&value) {
			// 				Ok(nickname) => {
			// 					println!("Got {:?} {:?}", std::str::from_utf8(key.as_ref()).unwrap(), value);

			// 					if self.queries.contains_key(&id) {
			// 						let mut state = STATE.lock().unwrap();
			// 						let peer_id = self.queries.remove(&id).expect("Message was not in queue");
			// 						state.nicknames.insert(peer_id.to_string(), nickname);
			// 					} 
			// 				}
			// 				Err(e) => {
			// 					println!("Failed to decode: {}", e);
			// 				}
			// 			}
			// 		}

			// 		kad::QueryResult::GetRecord(Ok(_)) => {}

			// 		kad::QueryResult::GetRecord(Err(err)) => {
			// 			println!("Failed to get record {:?}, error: {:?}", id, err);
			// 		}
					
			// 		kad::QueryResult::PutRecord(Ok(_)) => {
			// 			println!("Successfully put record {:?}", id);
			// 		}

			// 		kad::QueryResult::PutRecord(Err(err)) => {
			// 			println!("Failed to put record {:?}, error: {:?}", id, err);
			// 		}
					
			// 		_ => {}
			// 	}
			// }

            // SwarmEvent::Behaviour(ChatBehaviourEvent::Kademlia(_)) => {}

            // SwarmEvent::Behaviour(ChatBehaviourEvent::RequestResponse(
            //     request_response::Event::Message { message, .. },
            // )) => match message {
            //     request_response::Message::Request {
            //         request, channel, ..
            //     } => {
            //         self.event_sender
            //             .send(Event::InboundRequest {
            //                 request: request.0,
            //                 channel,
            //             })
            //             .await
            //             .expect("Event receiver not to be dropped.");
            //     }
            //     request_response::Message::Response {
            //         request_id,
            //         response,
            //     } => {
            //         let _ = self
            //             .pending_request_file
            //             .remove(&request_id)
            //             .expect("Request to still be pending.")
            //             .send(Ok(response.0));
            //     }
            // },

            // SwarmEvent::Behaviour(ChatBehaviourEvent::RequestResponse(
            //     request_response::Event::OutboundFailure {
            //         request_id, error, ..
            //     },
            // )) => {
            //     let _ = self
            //         .pending_request_file
            //         .remove(&request_id)
            //         .expect("Request to still be pending.")
            //         .send(Err(Box::new(error)));
            // }

            // SwarmEvent::Behaviour(ChatBehaviourEvent::RequestResponse(
            //     request_response::Event::ResponseSent { .. },
            // )) => {}

            // SwarmEvent::IncomingConnection { .. } => {}

            // SwarmEvent::ConnectionEstablished {
            //     peer_id, endpoint, ..
            // } => {
            //     if endpoint.is_dialer() {
			// 		eprintln!("Connection established with peer: {:?}", peer_id);
            //         if let Some(sender) = self.pending_dial.remove(&peer_id) {
            //             let _ = sender.send(Ok(()));
            //         }
            //     }
            // }

            // SwarmEvent::ConnectionClosed { .. } => {}

            // SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
            //     if let Some(peer_id) = peer_id {
            //         if let Some(sender) = self.pending_dial.remove(&peer_id) {
            //             let _ = sender.send(Err(Box::new(error)));
            //         }
            //     }
            // }

            // SwarmEvent::IncomingConnectionError { .. } => {}

            // SwarmEvent::Dialing {
            //     peer_id: Some(peer_id),
            //     ..
            // } => eprintln!("Dialing {peer_id}"),

            e => panic!("{e:?}"),
        }
    }

    async fn handle_command(&mut self, command: Command) {
        match command {		
			Command::PublishMessage { message } => {
				let mut state = STATE.lock().unwrap();
                let room = state.current_room.clone();
				println!("Publishing message to room: {:?}", room);
				let topic = gossipsub::IdentTopic::new(room);
				if let Err(err) = self.swarm.behaviour_mut().gossipsub.publish(topic.clone(), message.as_bytes()) {
					eprintln!("Error publishing message to topic {:?}: {:?}", topic, err);
				}
			}

            // Command::Dial {
            //     peer_id,
            //     peer_addr,
            //     sender,
            // } => {
            //     if let hash_map::Entry::Vacant(e) = self.pending_dial.entry(peer_id) {
            //         self.swarm
            //             .behaviour_mut()
            //             .kademlia
            //             .add_address(&peer_id, peer_addr.clone());
            //         match self.swarm.dial(peer_addr.with(Protocol::P2p(peer_id))) {
            //             Ok(()) => {
            //                 e.insert(sender);
            //             }
            //             Err(e) => {
            //                 let _ = sender.send(Err(Box::new(e)));
            //             }
            //         }
            //     } else {
            //         todo!("Already dialing peer.");
            //     }
            // }

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

            Command::RespondFile { file, channel } => {
                self.swarm
                    .behaviour_mut()
                    .request_response
                    .send_response(channel, FileResponse(file))
                    .expect("Connection to peer to be still open.");
            }
        }
    }
}


#[derive(Debug)]
enum Command {
    StartProviding {
        file_name: String,
        sender: oneshot::Sender<()>,
    },
    GetProviders {
        file_name: String,
        sender: oneshot::Sender<HashSet<PeerId>>,
    },
    RequestFile {
        file_name: String,
        peer: PeerId,
        sender: oneshot::Sender<Result<Vec<u8>, Box<dyn Error + Send>>>,
    },
    RespondFile {
        file: Vec<u8>,
        channel: ResponseChannel<FileResponse>,
    },
	PublishMessage {
		message: String,
	}
}

#[derive(Debug)]
pub(crate) enum Event {
    InboundRequest {
        request: String,
        channel: ResponseChannel<FileResponse>,
    },
}

/**
 * Part of the file exchange protocol for the application,
 * responsible for the request.
 */
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRequest(pub String);

/**
 * Part of the file exchange protocol for the application,
 * responsible for the response.
 */
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileResponse(pub Vec<u8>);
