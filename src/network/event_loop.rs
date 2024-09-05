use futures::channel::{mpsc, oneshot};
use futures::prelude::*;
use futures::StreamExt;

use libp2p::kad::store::MemoryStore;
use libp2p::kad::Behaviour;
use libp2p::{gossipsub, mdns};
use libp2p::{
    kad,
    request_response::{self, OutboundRequestId, ResponseChannel},
    swarm::{Swarm, SwarmEvent},
    PeerId,
};

use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use std::collections::{HashMap, HashSet};
use std::error::Error;

use crate::state::{GlobalState, STATE};
use std::sync::MutexGuard;
use super::behaviour::{ChatBehaviour, ChatBehaviourEvent};
use super::command::Command;



#[derive(Debug)]
pub enum Event {
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

/**
 * 
 */
pub struct EventLoop {
    pub swarm: Swarm<ChatBehaviour>,
    pub command_receiver: mpsc::Receiver<Command>,
    pub event_sender: mpsc::Sender<Event>,
    pub pending_dial: HashMap<PeerId, oneshot::Sender<Result<(), Box<dyn Error + Send>>>>,
    pub pending_start_providing: HashMap<kad::QueryId, oneshot::Sender<()>>,
    pub pending_get_providers: HashMap<kad::QueryId, oneshot::Sender<HashSet<PeerId>>>,
    pub pending_request_file:
        HashMap<OutboundRequestId, oneshot::Sender<Result<Vec<u8>, Box<dyn Error + Send>>>>,
}

/**
 * 
 */
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
            SwarmEvent::Behaviour(ChatBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                for (peer_id, multiaddr) in list {
                    // Discovered peer: peer_id at multiaddr.
                    let mut state = STATE.lock().unwrap();
                    
                    // Adding the peer to our  network.
                    self.swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                    self.swarm.behaviour_mut().kademlia.add_address(&peer_id, multiaddr);

                    // Fetching the nickname from kademlia
                    fetch_nickname(peer_id, &mut self.swarm.behaviour_mut().kademlia, &mut state);
                    // Fetching the rooms from kademlia
                    fetch_rooms(*self.swarm.local_peer_id(), &mut self.swarm.behaviour_mut().kademlia, &mut state);

                    // Create DM topic for the peers, though it will not be available unless they've accepted it previously TODO.
                    let ids = vec![state.peer_id.clone(), peer_id.to_string()];
                    let mut sorted_ids = ids;
                    sorted_ids.sort(); // Sort IDs alphabetically, to ensure the same topic for each peer.
    
                    // Create a topic string using a separator to ensure valid topic names
                    let topic = format!("/dm/{}", sorted_ids.join("_")); // Using underscore as a separator
                    let topic_id = gossipsub::IdentTopic::new(&topic.to_string());

                    // Subscribe to the DM topic
                    if let Err(e) = self.swarm.behaviour_mut().gossipsub.subscribe(&topic_id) {
                        eprintln!("Error subscribing to topic '{}': {:?}", topic_id, e);
                    }
                }
            },
            SwarmEvent::Behaviour(ChatBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                for (peer_id, multiaddr) in list {
                    // mDNS discoverd peer has expired: {peer_id}
                    self.swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                    self.swarm.behaviour_mut().kademlia.remove_address(&peer_id, &multiaddr);
                }
            },
            SwarmEvent::Behaviour(ChatBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                propagation_source: peer_id,
                message_id: _id,
                message,
            })) => {
                // Lock the state
                let mut state = STATE.lock().unwrap();

                // Fetching the nickname from the state (before update)
                let mut nickname = state.nicknames.get(&peer_id).cloned();

                // Fetching the nickname from kademlia
                fetch_nickname(peer_id, &mut self.swarm.behaviour_mut().kademlia, &mut state);

                // After fetching from kademlia, update the nickname if it's different
                let stored_peer_nickname = state.nicknames.get(&peer_id).cloned();
                if nickname != stored_peer_nickname {
                    nickname = stored_peer_nickname;
                }

                // Handle the message based on the updated nickname
                if nickname.is_none() {
                    println!("Unknown: {}", String::from_utf8_lossy(&message.data));
                } else {
                    let message = String::from_utf8_lossy(&message.data);
                    let current_room = format!("[{}]", state.current_room);

                    if message.starts_with("/dm") {
                        let message_stripped = message.strip_prefix("/dm").unwrap_or(&message);
                        println!("{}", message_stripped);
                    } else if message.starts_with(&current_room) {
                        println!("{}", message);
                    }
                }
            },
            SwarmEvent::NewListenAddr { address, .. } => {
                //println!("Local node is listening on {address}");
                if address.to_string().contains("/ip4/127.0.0.1/udp") {
                    let state = STATE.lock().unwrap();

                    // TODO 
                    let peer_id = self.swarm.local_peer_id().clone();
                    self.swarm.behaviour_mut().kademlia.add_address(&peer_id, address);

                    let nickname_bytes: Vec<u8> = serde_cbor::to_vec(&KademliaRecords::Nickname(state.nickname.to_string())).unwrap();
                    let key: String = state.peer_id.to_string();

                    let record = kad::Record {
                        key: kad::RecordKey::new(&key),
                        value: nickname_bytes,
                        publisher: None,
                        expires: None,
                    };

                    self.swarm.behaviour_mut().kademlia
                        .put_record(record, kad::Quorum::One)
                        .expect("");
                }
            }

            // Kademlia events!
            SwarmEvent::Behaviour(ChatBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                id,
                result,
                ..
            })) => {
                match result {
                    // Get record return result
                    kad::QueryResult::GetRecord(Ok(
                        kad::GetRecordOk::FoundRecord(kad::PeerRecord {
                            record: kad::Record { key, value, ..},
                            ..
                        })
                    )) => {
                        let mut state = STATE.lock().unwrap();
                        
                        match serde_cbor::from_slice::<KademliaRecords>(&value) {
                            
                            Ok(KademliaRecords::Nickname(nickname)) => {
                                if state.queries.contains_key(&id) {
                                    let peer_id = state.queries.remove(&id).expect("Message was not in queue");
                                    state.nicknames.insert(peer_id.clone(), nickname.clone());
                                    // TODO println!("{} has joined the chat!", nickname.clone());
                                    // TODO println!("Added peer {} with nickname {}", peer_id, nickname); 
                                }
                            }

                            Ok(KademliaRecords::Rooms(rooms)) => {
                                // Update local states and subscribe the user to all available rooms
                                for (room, messages) in rooms.iter() {
                                    if !state.rooms.contains_key(room) {
                                        state.rooms.insert(room.to_string(), messages.to_vec());
                                    }

                                    let topic = gossipsub::IdentTopic::new(room.to_string());
                                    self.swarm.behaviour_mut().gossipsub.subscribe(&topic).unwrap();
                                }
                            }

                            Err(e) => {
                                println!("Failed to decode nickname: {}", e); // Optional: Add this line to check decoding errors
                            }
                        }
                    }

                    // Get record return result Ok.
                    kad::QueryResult::GetRecord(Ok(_)) => {}

                    // Get record return result into an error.
                    kad::QueryResult::GetRecord(Err(err)) => {
                        println!("Failed to get record {:?}, error: {:?}", id, err);
                    }

                    // Successfully putting the record.
                    kad::QueryResult::PutRecord(Ok(_)) => {
                        println!("Successfully put record {:?}", id);
                    }

                    // Error putting the record.
                    kad::QueryResult::PutRecord(Err(_err)) => {
                        // ("Failed to put record {:?}, error: {:?}", id, err);
                    }

                    kad::QueryResult::StartProviding(_) => {
                        let sender: oneshot::Sender<()> = self
                            .pending_start_providing
                            .remove(&id)
                            .expect("Completed query to be previously pending.");
                        let _ = sender.send(());
                    }

                    kad::QueryResult::GetProviders(Ok(kad::GetProvidersOk::FoundProviders {
                        providers,
                        ..
                    })) => {
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

                    kad::QueryResult::GetProviders(Ok(
                        kad::GetProvidersOk::FinishedWithNoAdditionalRecord { .. },
                    )) => {}

                    _ => {}
                }
            }

            SwarmEvent::Behaviour(ChatBehaviourEvent::Kademlia(_)) => {}

            SwarmEvent::Behaviour(ChatBehaviourEvent::RequestResponse(
                request_response::Event::Message { message, .. },
            )) => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    // A request has been received.
                    println!("Request {:?}", request);

                    let filename = request.0;
                    let file_bytes = match File::open(filename).await {
                        Ok(mut file) => {
                            let mut buffer = Vec::new();
                            // read the file into a buffer
                            let _ = file.read_to_end(&mut buffer).await;
                            buffer
                        }
                        // if the file doesn't exist just send empty byte array in response
                        Err(_) => vec![],
                    };
                    // send the response to the file requester
                    self.swarm.behaviour_mut().request_response.send_response(channel, FileResponse(file_bytes)).unwrap();

                    // self.event_sender
                    //     .send(Event::InboundRequest {
                    //         request: request.0,
                    //         channel,
                    //     })
                    //     .await
                    //     .expect("Event receiver not to be dropped.");
                }

                // Recieving a response.
                request_response::Message::Response {
                    request_id,
                    response,
                } => {
                    // let _ = self
                    //     .pending_request_file
                    //     .remove(&request_id)
                    //     .expect("Request to still be pending.")
                    //     .send(Ok(response.0));

                    // response has the vector of bytes sent from the file server
                    // here we just print, but you could save it or do something else with it
                    println!("response {:?}", response);
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
            SwarmEvent::ConnectionEstablished {
                peer_id, ..
            } => {
                // if we've established a connection, then save the peer id in `other_peer_id` for later use
                let other_peer_id = Some(peer_id);
                println!("{:?}", peer_id);
            }
            other => {
                println!("Unhandled {:?}", other);
            }
            _ => {}
            //e => panic!("{e:?}"),
        }
    }

    

    pub async fn handle_command(&mut self, command: Command) {
        // lock the state at the beginning to ensure a consistent state!
        let mut state: MutexGuard<GlobalState> = STATE.lock().unwrap();

        // then handle the command that was passed in.
        match command {
            Command::Error => {
                println!("Please use the command correctly!")
            }
    
            Command::DirectMessage {
                peer_nickname,
                message,
            } => {
                // if !state.friends.contains(&peer_nickname) {
                //     println!("You are not able to direct message {}, please try /trade as they need to accept your response.", peer_nickname);
                // } else  {
                // the current user's peer id.
                let local_peer_id = self.swarm.local_peer_id().clone();
    
                // find the peer ID associated with the given nickname.
                let peer_id = state.nicknames.iter().find_map(|(id, nickname)| {
                    if nickname == &peer_nickname {
                        Some(id.clone())
                    } else {
                        None
                    }
                });
    
                // if this peer exists, send the message to the direct message that they are both subscribed to.
                if let Some(peer_id) = peer_id {
                    // create the message to send: the user's nickname and the message
                    let ids = vec![local_peer_id.to_base58(), peer_id.to_base58()];
                    let mut sorted_ids = ids;
                    sorted_ids.sort(); // Sort IDs alphabetically
    
                    // Create a topic string using a separator to ensure valid topic names
                    let topic = format!("/dm/{}", sorted_ids.join("_")); // Using underscore as a separator
                    let topic_id = gossipsub::IdentTopic::new(&topic.to_string());
    
                    let data = ("/dm".to_string() + "[From]: " + &state.nickname.to_string() + &message).as_bytes().to_vec();
                    self.swarm
                        .behaviour_mut()
                        .gossipsub
                        .publish(topic_id, data)
                        .expect("Failed to publish direct message.");
    
                    println!("[To] {}: {}", peer_nickname, message);
                } else {
                    println!("Peer with nickname {} not found.", peer_nickname);
                }
                //}
            }
    
            Command::SetNickname { new_nickname } => {
                let old_nickname = state.nickname.clone();
                state.nickname = new_nickname.clone();
    
                // Update the nickname in Kademlia (DHT)
                let peer_id = self.swarm.local_peer_id().clone();
                let key = kad::RecordKey::new(&peer_id.to_string());
                let record = kad::Record {
                    key,
                    value: serde_cbor::to_vec(&KademliaRecords::Nickname(new_nickname.to_string())).unwrap(),
                    publisher: None,
                    expires: None,
                };
    
                if let Err(e) = self.swarm
                    .behaviour_mut()
                    .kademlia
                    .put_record(record, kad::Quorum::One)
                {
                    eprintln!("Failed to update nickname in DHT: {:?}", e);
                } else {
                    println!(
                        "Nickname changed from '{}' to '{}'",
                        old_nickname, new_nickname
                    );
                }
            }
    
            Command::Message { message, room } => {
                // Create a new topic
                let topic = gossipsub::IdentTopic::new(room.clone());

                // Format the message with room and nickname
                let formatted_message = format!("[{}] {}: {}", room, state.nickname, message);

                // Publish the message
                if let Err(e) = self.swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(topic.clone(),  formatted_message.as_bytes())
                {
                    if e.to_string().contains("InsufficientPeers") {
                        println!("[{}] You: {}", topic.to_string(), message);
                    } else {
                        println!("[Error]: Unable to publish your message.");
                    } 
                } else {
                    println!("[{}] You: {}", topic.to_string(), message);
                }
            }
    
            Command::ListPeers => {
                println!("Listing all available peers, would you like to contact one?");
                let peers = state.nicknames.clone(); // access the state safely
                for (_peer_id, nickname) in peers {
                    if state.nickname == nickname {
                        continue;
                    }
                    println!("{}", nickname);
                }
            }
    
            Command::ListFiles => {
                let files = state.files.clone();
                if files.len() <= 0 {
                    println!("Currently there are no available files.");
                } else {
                    println!("Listing all available files, with their respective owners:");
                    for (file, owner) in state.files.iter() {
                        println!("{} - {}", file, state.nicknames.get(owner).unwrap());
                    }
                }
            }
    
            // Creates a room/chat with the name that was given.
            Command::CreateRoom { name } => {
                let peer_id = state.peer_id.clone();
                let current_rooms = state.rooms.clone();

                if current_rooms.contains_key(&name) {
                    println!("Unable to create room, {} already exists.", name);
                } else if (&name).is_empty() {
                    println!("Unable to create room, name is empty.");
                } else if name == "Global Chat" {
                    println!("Unable to create room, Global Chat already exists.");
                } else {
                    // Update the local state with the new room.
                    state.rooms.insert(name.clone(), vec![]);
                    let topic = gossipsub::IdentTopic::new(name.clone());
                    
                    let seralized_rooms = serde_cbor::to_vec(&KademliaRecords::Rooms(state.rooms.clone())).unwrap();

                    // Create Kademlia record for room TODO
                    let key = kad::RecordKey::new(&"available_rooms");
                    let record = kad::Record {
                        key,
                        value: seralized_rooms,
                        publisher: None,
                        expires: None
                    };

                    // Insert the room record into Kademlia.
                    self.swarm.behaviour_mut().kademlia.put_record(record, kad::Quorum::One).expect("");

                    // Fetching the rooms from kademlia...
                    fetch_rooms(*self.swarm.local_peer_id(), &mut self.swarm.behaviour_mut().kademlia, &mut state);

                    println!("Creating room '{}', please change to this room when required.", name);
                    self.swarm.behaviour_mut().gossipsub.subscribe(&topic).expect("");
                }
            }
    
            Command::ChangeRoom { name } => {
                let current_rooms = state.rooms.clone();
                let current_room = state.current_room.clone();

                if current_room == name {
                    println!("You're already in the {} room.", name);
                } else if (&name).is_empty() {
                    println!("Unable to join a room that does not exist.");
                } else if !current_rooms.contains_key(&name) {
                    println!("Unable to join a room that does not exist. ");
                } else {
                    // Change to the new chat!
                    state.current_room = name.clone();
                    println!("Changing to the {} room.", name);

                    // Subscribe to the new chat!
                    let topic = gossipsub::IdentTopic::new(name);
                    self.swarm.behaviour_mut().gossipsub.subscribe(&topic).expect("");
                }
            }
    
            Command::ListRooms => {
                // Fetching the rooms from kademlia...
                fetch_rooms(*self.swarm.local_peer_id(), &mut self.swarm.behaviour_mut().kademlia, &mut state);

                // After fetching from kademlia, print out all available rooms
                println!("Listing all available rooms:");
                for (room, _peer_ids) in state.rooms.iter() {
                    println!("{}", room);
                }
            }
    
            Command::Exit => {
                println!("Exiting SwapBytes. Goodbye!");
                std::process::exit(0);
            }
    
            Command::TradeRequest {
                peer_nickname,
                message,
            } => {
                // the current user's peer id and their nickname!
                let local_peer_id = self.swarm.local_peer_id().clone();
                let local_nickname = state.nickname.clone();
    
                // need the peer id of the peer we want to trade with along with that we have their nickname!
                let peer_id = state.nicknames.iter().find_map(|(id, nickname)| {
                    if nickname == &peer_nickname {
                        Some(id.clone())
                    } else {
                        None
                    }
                });
    
                // send a trade request?
                // send a trade response?
                // need to get a yes... for the trade...
                // how to ensure that the peer is okay with this?
                println!("Starting trade request!");
            }
    
            Command::TradeResponse {
                peer_nickname,
                message,
            } => {
                // the current user's peer id and their nickname!
                let local_peer_id = self.swarm.local_peer_id().clone();
                let local_nickname = state.nickname.clone();
    
                // need the peer id of the peer we want to trade with along with that we have their nickname!
                let peer_id = state.nicknames.iter().find_map(|(id, nickname)| {
                    if nickname == &peer_nickname {
                        Some(id.clone())
                    } else {
                        None
                    }
                });
    
                // if message.contains("yes") {
                //     // if the message is yes, then commence create a direct msg topic!
                //     println!("Can now direct message one another??!");
                //     state.friends.push(peer_nickname.clone()); // i need to do this for both and im not sure how to..
                // } else {
                //     println!("Trade request denied, you still cannot direct message {}", peer_nickname);
                // }
                println!("Sending response to trade request!");
            }
    
            Command::Help => {
                // Print available commands and their usage
                println!("Available commands:");
                println!("/trade <peer_nickname> [message] - Send a trade request");
                println!("/dm <peer_nickname> <message> - Send a direct message");
                println!("/list-files - List available files");
                println!("/list-peers - List available peers");
                println!("/set-nickname <new_nickname> - Change your nickname");
                println!("/post-offer - Post a file offer");
                println!("/request-file - Request a file");
                println!("/accept-file - Accept a file offer");
                println!("/cancel-file - Cancel a file transfer");
                println!("/connect - Connect to a peer");
                println!("/create-room - Create a new chat room");
                println!("/change-room - Change the current chat room");
                println!("/list-rooms - List available chat rooms");
                println!("/exit - Exit the application");
            }
    
            Command::Unknown => {
                println!("Unknown command! Type /help for a list of available commands.");
            }
            
            Command::StartProviding { file_name, sender } => {
                println!("You are now providing the file: {}.", file_name);
                state.files.insert(file_name.clone(), *self.swarm.local_peer_id());

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

            // We want to request a file.
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


// Defines all ... stored in the Kademlia DHT (TODO)
#[derive(Serialize, Deserialize)]
pub enum KademliaRecords {
    Nickname(String), 
    Rooms(HashMap<String, Vec<String>>)
}

// This function handles nickname retrieval via Kademlia
fn fetch_nickname(peer_id: PeerId, kademlia: &mut Behaviour<MemoryStore>, state: &mut MutexGuard<GlobalState>) {
    let key = kad::RecordKey::new(&peer_id.to_string());
    let query_id = kademlia.get_record(key);
    state.queries.insert(query_id, peer_id);
}

// This function handles room retrieval via Kademlia
fn fetch_rooms(peer_id: PeerId, kademlia: &mut Behaviour<MemoryStore>, state: &mut MutexGuard<GlobalState>) {
    let key = kad::RecordKey::new(&"available_rooms");
    let query_id: kad::QueryId = kademlia.get_record(key);
    state.queries.insert(query_id, peer_id);
}