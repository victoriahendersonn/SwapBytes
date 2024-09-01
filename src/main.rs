use std::{collections::HashMap, error::Error, io::{ Read, Write }, sync::Arc, time::Duration};
use color_eyre::owo_colors::colors::xterm::UserBlack;
use futures::StreamExt;
use libp2p::{ gossipsub, identity, kad::{self, store::MemoryStore, Mode, QueryId}, mdns, noise, request_response::{self, ProtocolSupport}, swarm::{ NetworkBehaviour, SwarmEvent }, tcp, yamux, PeerId, StreamProtocol };
use serde::{Deserialize, Serialize};
use tokio::{ io::{ self, AsyncBufReadExt }, select };


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


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // let keypair = identity::Keypair::generate_ed25519();

    // importing libp2p -> creating a SwarmBuilder with a new identity -> creates a random key pair!
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
    
    // telling the swarm that the distributed hash table is acting as a server for requests that other 
    // nodes will be asking for keys about?
    swarm.behaviour_mut().kademlia.set_mode(Some(Mode::Server));


    // creating a topic 
    let topic = gossipsub::IdentTopic::new("global-chat");
    // subscribing to the topic
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    // will listen on local host over two different protocols (UDP and TCP)
    // will randomly select a port for TCP 
    //swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    // QUIC is built on top of UDP -> we have to tell it that it has to be through QUIC
    swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
    // ^^ what ports do we want to listen on?

    // adding this for standard user input!
    // telling the standard input that it has to be wrapped around the tokio one
    // the buffer reader will read that line in but one line at a time. 
    let mut stdin = io::BufReader::new(io::stdin()).lines();
    println!("Please enter a nickname:");
    let nickname: String = stdin.next_line().await.unwrap().unwrap().to_string();
    let mut queries: HashMap<QueryId, (PeerId, String)> = HashMap::new();

    println!("Enter chat messages one line at a time:");

    // messages will be coming in, as they are coming in we need to handle them as they happen.
    // tokio has 'future's which will help with this.
    // select the next future that has been completed.
    loop {
        select! {
            // is there a next line? if so go through that branch, if not... don't.
            Ok(Some(line)) = stdin.next_line() => {
                // take the return value, if it matches error type, then push it into the error variable
                // and print the line saying there was an error.
                if let Err (err) = swarm.behaviour_mut().gossipsub.publish(topic.clone(), line.as_bytes()) {
                    println!("Error publishing: {:?}", err);
                }
            }

            // match event to the event that actually happened.
            event = swarm.select_next_some() => match event {
                // is someone listening to me? 

                SwarmEvent::NewListenAddr { address, .. } => {
                    if address.to_string().contains("/ip4/127.0.0.1/udp") {
                        let peer_id = swarm.local_peer_id().clone();
    
                        swarm.behaviour_mut().kademlia.add_address(&peer_id, address);

                        // Serialize nickname.
                        let nickname_bytes = serde_cbor::to_vec(&nickname).unwrap();
                        
                        let key = &peer_id.to_string();

                        let record = kad::Record{
                            key: kad::RecordKey::new(&key),
                            value: nickname_bytes,
                            publisher: None,
                            expires: None,
                        };

                        match swarm.behaviour_mut().kademlia.put_record(record, kad::Quorum::One) {
                            Ok(_) => println!("Successfully put record {:?}", nickname),
                            Err(err) => {
                                println!("Failed to put record {err:?}");
                            }
                        }
                    }
                    // println!("Your node is listening on {address}");
                }
                   
                SwarmEvent::Behaviour(ChatBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, multiaddr) in list {
                        // println!("mdns discovered peer: {peer_id}, listening on {multiaddr}");
                        // peers have been discovered! time to add them.
                        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                        swarm.behaviour_mut().kademlia.add_address(&peer_id, multiaddr);
                    }
                },

                SwarmEvent::Behaviour(ChatBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                    // removing an expired peer!
                    for (peer_id, multiaddr) in list {
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                        swarm.behaviour_mut().kademlia.remove_address(&peer_id, &multiaddr);
                    }
                },

                SwarmEvent::Behaviour(ChatBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source: peer_id,
                    message_id: _id, 
                    message,
                })) =>
                    {
                        if let Ok(message) = String::from_utf8(message.data.clone()) {
                            let query_id = swarm.behaviour_mut().kademlia.get_record(kad::RecordKey::new(&peer_id.to_string()));
                            queries.insert(query_id, (peer_id.clone(), message));
                    }
                }
                ,

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

                            if let Some((peer_id, message)) = queries.remove(&id) {
                                match serde_cbor::from_slice::<String>(&value) {
                                    Ok(nickname) => {
                                        println!("{}: {}", nickname, message);
                                    }
                                    Err(_) => {
                                        println!("Failed to decode nickname for peer {}, but received: {}", peer_id, message);
                                    }
                                }
                            }
                        }

                        kad::QueryResult::GetRecord(Ok(_)) => {}

                        kad::QueryResult::GetRecord(Err(err)) => {
                            println!("Failed to get record {err:?}");
                        }
                        
                        kad::QueryResult::PutRecord(Ok(_)) => {
                            println!("Successfully put record {:?}", id);
                        }

                        kad::QueryResult::PutRecord(Err(err)) => {
                            println!("Failed to put record {err:?}");
                        }
                        
                        _ => {}
                    }
                }

                _ => {} // if the event doesn't match anything, just do nothing.
            }
        }
    }
}