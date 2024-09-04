use futures::channel::mpsc;
use futures::prelude::*;

use libp2p::kad::store::MemoryStore;
use libp2p::{ 
    kad,
    noise,
    request_response::{self, ProtocolSupport},
    swarm::NetworkBehaviour,
    tcp, yamux,
};

use libp2p::{gossipsub, mdns, StreamProtocol};
use std::error::Error;
use std::time::Duration;

use tokio::io::{self, AsyncBufReadExt};

use super::client::Client;
use super::event_loop::{Event, EventLoop, FileRequest, FileResponse};
use crate::state::STATE;

// The main entry point of the network which defines the behaviour of the libp2p application.
#[derive(NetworkBehaviour)]
pub struct ChatBehaviour {
    // what is chat behaviour event then?
    // chat behaviour event gets added automatically (the compiler makes it!)

    // mDNS behaviour for local peer discovery
    pub mdns: mdns::tokio::Behaviour,
    
    // Gossipsub behaviour for pub/sub message propagation.
    pub gossipsub: gossipsub::Behaviour,

    // Request-Response behaviour for exchanging files.
    pub request_response: request_response::cbor::Behaviour<FileRequest, FileResponse>,

    // Kademlia DHT for p2p ... TODO
    pub kademlia: kad::Behaviour<MemoryStore>,
}

pub async fn swarm() -> Result<(Client, impl Stream<Item = Event>, EventLoop), Box<dyn Error>> {
    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )? // how are we going to send the bits back and forth across our network?
        .with_quic() // will upgrade from TCP to QUIC if it can...
        .with_behaviour(|key| {
            Ok(ChatBehaviour {
                mdns: mdns::tokio::Behaviour::new(
                    mdns::Config::default(),
                    key.public().to_peer_id(),
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

    // Setting up network specifics, along with the address to listen on.
    swarm.behaviour_mut().kademlia.set_mode(Some(kad::Mode::Server));
    swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;

    // Adding the Global Chat to the network.
    let topic = gossipsub::IdentTopic::new("Global Chat");
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    // Asking for the users's nickname.
    println!("Please enter a nickname:");
    let mut stdin: io::Lines<io::BufReader<io::Stdin>> = io::BufReader::new(io::stdin()).lines();

    {
        // Adding user and network specific information to the local storage.
        let mut state = STATE.lock().unwrap();
        let mut new_nickname = stdin.next_line().await.unwrap().unwrap();
        new_nickname = new_nickname.trim_start().trim_end().to_string();
        state.nickname = new_nickname.clone();

        println!(
            "Welcome to SwapBytes, {}! Please enter chat messages one line at a time or type \\help for a list of available commands.",
            state.nickname
        );

        let peer_id = swarm.local_peer_id().clone();
        state.peer_id = peer_id.to_string();
        state.nicknames.insert(peer_id, new_nickname.clone());

        state.current_room = "Global Chat".to_string();
        state.rooms.insert("Global Chat".to_string(), vec![peer_id.to_string()]);
    }

    // Setting up command and event channels.
	let (command_sender, command_receiver) = mpsc::channel(0);
    let (event_sender, event_receiver) = mpsc::channel(0);

    // Returning the Client, event stream, and EventLoop for the rest of the application to use
    // and call to when necessary.
    Ok((
        Client {
            sender: command_sender,
        },
        event_receiver,
        EventLoop::new(swarm, command_receiver, event_sender),
    ))
}