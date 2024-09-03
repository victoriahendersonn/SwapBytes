use futures::StreamExt;
use libp2p::{
    noise, swarm::{NetworkBehaviour, SwarmEvent}, tcp, yamux, Multiaddr, PeerId,
    request_response::{self, ProtocolSupport}, StreamProtocol,
};
use serde::{Deserialize, Serialize};
use tokio::{io::{self, stdin, AsyncBufReadExt, BufReader, AsyncReadExt}, select, fs::File};
use std::{error::Error, time::Duration};
use clap::Parser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // parses the command line options using clap
    let opt = Opt::parse();
    
    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|_key| MyBehaviour{
            request_response: request_response::cbor::Behaviour::new(
                [(
                    StreamProtocol::new("/file-exchange/1"),
                    ProtocolSupport::Full,
                )],
                request_response::Config::default(),
            )
        })?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(7200)))
        .build();

    let listen_port = opt.port.unwrap_or("0".to_string());
    let multiaddr = format!("/ip4/0.0.0.0/tcp/{listen_port}");
    swarm.listen_on(multiaddr.parse()?)?;

    // if this is a file request peer, then dial the peer that serves the files
    if let Some(peer) = opt.peer {
        swarm.dial(peer).unwrap();
    }

    let mut stdin: io::Lines<BufReader<io::Stdin>> = BufReader::new(stdin()).lines();

    let mut other_peer_id: Option<PeerId> = None;

    loop {
        select! {
            Ok(Some(line)) = stdin.next_line() => { 
                // if we are a file requesting peer then request the file name typed by the user,
                // otherwise ignore.
                if let Some(peer_id) = other_peer_id {
                    swarm.behaviour_mut().request_response.send_request(&peer_id, FileRequest(line.to_string()));
                }
            }
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Listening on {}", address);
                }
                SwarmEvent::Behaviour(MyBehaviourEvent::RequestResponse(
                    request_response::Event::Message { message, .. },
                )) => match message {
                    request_response::Message::Request {
                        request, channel, ..
                    } => {
                        // a request has been received
                        println!("request {:?}", request);
                        let filename = request.0;
                        let file_bytes = match File::open(filename).await {
                            Ok(mut file) => {
                                let mut buffer = Vec::new();
                                // read the file into a buffer
                                file.read_to_end(&mut buffer).await?;
                                buffer
                            }
                            // if the file doesn't exist just send empty byte array in response
                            Err(_) => vec![],
                        };
                        // send the response to the file requester
                        swarm.behaviour_mut().request_response.send_response(channel, FileResponse(file_bytes)).unwrap();
                    }
                    request_response::Message::Response {
                        response, ..
                    } => {
                        // response has the vector of bytes sent from the file server
                        // here we just print, but you could save it or do something else with it
                        println!("response {:?}", response);
                    }
                }
                SwarmEvent::ConnectionEstablished {
                    peer_id, ..
                } => {
                    // if we've established a connection, then save the peer id in `other_peer_id` for later use
                    other_peer_id = Some(peer_id);
                    println!("{:?}", peer_id);
                }
                other => {
                    println!("Unhandled {:?}", other);
                }
            }
        }
    }
}

// behaviour for the network (there's no discovery in this example)
#[derive(NetworkBehaviour)]
struct MyBehaviour {
    request_response: request_response::cbor::Behaviour<FileRequest, FileResponse>,
}

// file exchange protocol for our app
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRequest(String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileResponse(Vec<u8>);

// Command line parsing
#[derive(Parser, Debug)]
#[clap(name = "libp2p request response example")]
struct Opt {
    #[clap(long)]
    port: Option<String>,

    #[clap(long)]
    peer: Option<Multiaddr>,
}