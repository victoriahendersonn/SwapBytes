use futures::stream::StreamExt;
use libp2p::kad;
use libp2p::kad::store::MemoryStore;
use libp2p::kad::Mode;
use libp2p::{
    mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncBufReadExt;
use tokio::{io, select};
use std::error::Error;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|key| {
            Ok(Behaviour {
                mdns: mdns::tokio::Behaviour::new(
                    mdns::Config::default(),
                    key.public().to_peer_id(),
                )?,
                kademlia: kad::Behaviour::new(
                    key.public().to_peer_id(),
                    MemoryStore::new(key.public().to_peer_id()),
                )
            })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();
    
    // telling the swarm that the distributed hash table is acting as a server for requests that other 
    // nodes will be asking for keys about?
    swarm.behaviour_mut().kademlia.set_mode(Some(Mode::Server));

    let mut stdin = io::BufReader::new(io::stdin()).lines();

    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    loop {
        select! {
            Ok(Some(line)) = stdin.next_line() => handle_input_line(line, &mut swarm.behaviour_mut().kademlia)?,
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Listening in {address:?}");
                },
                SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, multiaddr) in list {
                        swarm.behaviour_mut().kademlia.add_address(&peer_id, multiaddr);
                    }
                }
                SwarmEvent::Behaviour(BehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed { result, .. } )) => {
                    match result {
                        kad::QueryResult::GetRecord(Ok(
                            kad::GetRecordOk::FoundRecord(kad::PeerRecord {
                                record: kad::Record { key, value, .. },
                                ..
                            })
                        )) => {
                            match serde_cbor::from_slice::<BookInfo>(&value) {
                                Ok(book_info) => {
                                    println!(
                                        "Got record {:?} {:?}", 
                                        std::str::from_utf8(key.as_ref()).unwrap(),
                                        book_info,
                                    )
                                }
                                Err(e) => {
                                    println!("Error deserializing: {e:?}");
                                }
                            }
                        }
                        kad::QueryResult::GetRecord(Ok(_)) => {}
                        kad::QueryResult::GetRecord(Err(err)) => {
                            println!("Failed to get record: {err:?}");
                        }
                        kad::QueryResult::PutRecord(Ok(kad::PutRecordOk { key } )) => {
                            println!("Succesfully put record {:?}", std::str::from_utf8(key.as_ref()).unwrap());
                        }
                        kad::QueryResult::PutRecord(Err(err)) => {
                            println!("Failed to put record: {err:?}");
                        }
                        _ => {}
                    }
                },
                _ => {}
            }
        }
    }

}


fn handle_input_line(
    line: String, 
    kademlia: &mut kad::Behaviour<MemoryStore>,
) -> Result<(), Box<dyn Error>> {
    let args = split_string(&line);

    let cmd = if let Some(cmd) = args.get(0) {
        cmd
    } else {
        println!("No command given");
        return Ok(());
    };

    match cmd.as_str() {
        "PUT_BOOK" => {
            // PUT_BOOK "learning rust" "ben adams" mystery 100
            if args.len() < 5 || args[1].len() == 0 {
                println!("Not enough arguments");
                return Ok(());
            }
            let title = args[1].clone();
            let pages: u32 = match args[4].parse::<u32>() {
                Ok(p) => p,
                Err(_) => {
                    println!("invalid pages");
                    return Ok(());
                }
            };
            let book_info = BookInfo {
                author: args[2].clone(),
                genre: args[3].clone(), 
                pages,
            };
            let book_info_bytes = serde_cbor::to_vec(&book_info)?;
            let record = kad::Record {
                key: kad::RecordKey::new(&title),
                value: book_info_bytes,
                publisher: None,
                expires: None,
            };
            kademlia
                .put_record(record, kad::Quorum::One)
                .expect("Failed to store locally.");
         },
        "GET_BOOK" => {
            if args.len() < 2 || args[1].len() == 0 {
                println!("Not enough arguments");
                return Ok(());
            }

            let title = args[1].clone();
            let key = kad::RecordKey::new(&title);
            kademlia.get_record(key);
        }
        _ => { 
            println!("unexpected command");
        }
    }

    Ok(())
}


#[derive(NetworkBehaviour)]
struct Behaviour {
    mdns: mdns::tokio::Behaviour,
    kademlia: kad::Behaviour<MemoryStore>,
}

// not including title as part of the book info because that will be the key :)
#[derive(Debug, Serialize, Deserialize)]
struct BookInfo {
    author: String,
    genre: String, 
    pages: u32,
}


// this splits a string into a set of string but splits it on spaces. "" will be individual items.
fn split_string(input: &str) -> Vec<String> {
    let re = Regex::new(r#""([^"]*)"|\S+"#).unwrap();
    re.captures_iter(input)
        .map(|cap| cap.get(0).unwrap().as_str().to_string())
        .collect()
}
