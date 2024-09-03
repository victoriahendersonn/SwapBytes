use futures::{channel::mpsc, prelude::*};
use libp2p::{
    gossipsub, kad::{self, QueryId}, mdns, noise, request_response::{self, ProtocolSupport}, swarm::{NetworkBehaviour, SwarmEvent}, tcp, yamux, PeerId, StreamProtocol
};
use regex::Regex;
use tokio::{io, io::AsyncBufReadExt, select};
use std::{ collections::HashMap, error::Error, time::Duration};
use serde::{Deserialize, Serialize};
use libp2p::kad::Mode;

/**
 * Determines the behaviour of the chat.
 */
#[derive(NetworkBehaviour)]
pub struct ChatBehaviour {
    pub mdns: mdns::tokio::Behaviour,
    pub gossipsub: gossipsub::Behaviour,
    pub request_response: request_response::cbor::Behaviour<FileRequest, FileResponse>,
    pub kademlia: kad::Behaviour<kad::store::MemoryStore>,
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
 * Builds a Swarm, which will contain the state of the network as a whole - all of the 
 * behaviour of a libp2p network can be controlled through the swarm.
 * 
 * It will contain all active and pending connections to remotes and manages the state 
 * of all the substreams that have been opened, along with all the upgrades that were
 * built upon these substreams.
 */
pub async fn swarm() -> Result<(), Box<dyn Error>> {
    // now to create the swarm
    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
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
                kademlia: kad   ::Behaviour::new(
                    key.public().to_peer_id(),
                    kad::store::MemoryStore::new(key.public().to_peer_id()),
                ),
            })
        })?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(120)))
        .build();

	let mut stdin = io::BufReader::new(io::stdin()).lines();
	println!("Enter your nickname");
	let nickname = stdin.next_line().await.unwrap().unwrap();
    let mut has_set_name = false;
    let mut pending_queries: HashMap<QueryId, (PeerId, String)> = HashMap::new();
    let self_peer_id = swarm.local_peer_id().clone();

    let global_chat = gossipsub::IdentTopic::new("global-chat"); // Define the global chat.
    swarm.behaviour_mut().gossipsub.subscribe(&global_chat)?; // Subscribe to the global chat.
	swarm.behaviour_mut().kademlia.set_mode(Some(Mode::Server));

    // Listen on specified TCP and UDP ports
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;

    loop {
		select! {
			Ok(Some(line)) = stdin.next_line() =>  {
				//if line starts with / then it is a command
				if line.starts_with("/") {
					handle_command(line, &mut swarm, self_peer_id)?;
				} else {
					// Publish the message to the chat topic
					if let Err(err) = swarm.behaviour_mut().gossipsub.publish(global_chat.clone(), line.as_bytes()) {
						println!("Error publishing: {:?}", err);
					}
				}
			}
			// Handle events from the swarm
			event = swarm.select_next_some() => match event {

				SwarmEvent::NewListenAddr { address, ..} => {
					println!("Your node is listening on {address}");
				}

				SwarmEvent::Behaviour(ChatBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
					for (peer_id, multiaddr) in list {
						println!("mDNS discovered peer: {peer_id}, listening on {multiaddr}");
						// Add discovered peers to GossipSub
						swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
						swarm.behaviour_mut().kademlia.add_address(&peer_id, multiaddr);
						//if user has not set nickname
						if !has_set_name {
							let nickname_record = kad::Record {
								key: kad::RecordKey::new(&self_peer_id.to_string()),
								value: nickname.as_bytes().to_vec(),
								publisher: None,
								expires: None,
							};
							match swarm.behaviour_mut().kademlia.put_record(nickname_record, kad::Quorum::One) {
								Ok(_) => {
									// If the record is stored successfully, set has_set_name to true
									has_set_name = true;
								}
								Err(e) => {
									eprintln!("Failed to store record: {:?}", e);
								}
							}
						}
					}
				}

				SwarmEvent::Behaviour(ChatBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
					for (peer_id, multiaddr) in list {
						// Remove expired peers from GossipSub
						swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
						swarm.behaviour_mut().kademlia.remove_address(&peer_id, &multiaddr);
					}
				}

				SwarmEvent::Behaviour(ChatBehaviourEvent::Gossipsub(gossipsub::Event::Message {
					propagation_source: peer_id,
					message, ..
				})) => {
					{
						if let Ok(msg) = String::from_utf8(message.data.clone()) {
							// Start a query to get the nickname from the DHT
							let query_id = swarm.behaviour_mut().kademlia.get_record(kad::RecordKey::new(&peer_id.to_string()));
							
							// Store the message and the peer ID with the query ID for later use
							pending_queries.insert(query_id, (peer_id.clone(), msg));
						}
					}
				}

				SwarmEvent::Behaviour(ChatBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {id, result, ..})) => {
					match result {
						// Get record return result
						kad::QueryResult::GetRecord(Ok(
							kad::GetRecordOk::FoundRecord(kad::PeerRecord {
								record: kad::Record { value, ..},
								..
							})
						)) => {
							if let Some((peer_id, msg)) = pending_queries.remove(&id) {
								match std::str::from_utf8(&value) {
									Ok(nickname) => {
										println!("{nickname}: {msg}");
									}
									Err(_) => {
										println!("Failed to decode nickname for peer {peer_id}, but received: {msg}");
									}
								}
							}
						}
	
						kad::QueryResult::GetRecord(Ok(_)) => {}
						kad::QueryResult::GetRecord(Err(err)) => {
							println!("Failed to get record {err:?}");
						}
						kad::QueryResult::PutRecord(Ok(kad::PutRecordOk {key })) => {
							println!("Successfully put record {:?}", std::str::from_utf8(key.as_ref()).unwrap());
						}
						kad::QueryResult::PutRecord(Err(err)) => {
							println!("Failed to put record {err:?}");
						}
						_ => {}
					}
				}
				
				_ => {}
			}
		}
	}
}

pub fn split_string(input: &str) -> Vec<String> {
    let re = Regex::new(r#""([^"]*)"|\S+"#).unwrap();
    re.captures_iter(input)
        .map(|cap| cap.get(0).unwrap().as_str().to_string())
        .collect()
}

pub fn handle_command(
    line: String,
    swarm: &mut libp2p::Swarm<ChatBehaviour>,
    self_peer_id: PeerId,
) -> Result<(), Box<dyn Error>> {
    let args = split_string(&line);
    let kademlia = &mut swarm.behaviour_mut().kademlia;


    let cmd = if let Some(cmd) = args.get(0) {
        cmd 
    } else {
        println!("No command given");
        return Ok({});
    };

    match cmd.as_str() {
        "/help" => {
            println!("Available commands:");
            println!("/help - Show this help message");
            println!("/peers - List all connected peers");
            println!("/nickname <nickname> - Set your nickname");
            // println!("/dial <peer_id> - Dial a peer by peer ID");
        }
        "/peers" => {
            let peers = swarm.connected_peers();
            println!("Connected peers:");
            for peer in peers {
                println!("{}", peer);
            }
        }
        "/nickname" =>{
            let local_peer_id_record = kad::RecordKey::new(&self_peer_id.to_string());
            let record = kad::Record {
                key: local_peer_id_record,
                value: args[1].as_bytes().to_vec(),
                publisher: None,
                expires: None,
            };
            kademlia
                .put_record(record, kad::Quorum::One)
                .expect("Failed to store record locally.");
        }
        // dial peerid
        // "/dial" => {
        //     if let Some(peer_id) = args.get(1) {
        //         let peer_id = PeerId::from_str(peer_id)?;
        //         let addr = kademlia.get_address(&peer_id)?;
        //         swarm.dial_addr(addr)?;
        //     } else {
        //         println!("No peer ID given");
        //     };
        // }
        _=> {
            println!("Unexpected command");
        }
    }
    Ok({})
}