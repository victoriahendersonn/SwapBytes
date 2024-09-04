use futures::channel::{mpsc, oneshot};
use futures::prelude::*;
use futures::StreamExt;

use libp2p::kad::store::MemoryStore;
use libp2p::{
    core::Multiaddr,
    identity, kad,
    noise,
    request_response::{self, OutboundRequestId, ProtocolSupport, ResponseChannel},
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    tcp, yamux, PeerId,
};

use libp2p::{gossipsub, mdns, StreamProtocol};
use std::error::Error;
use std::time::Duration;

use tokio::{
    io::{self, AsyncBufReadExt},
};


use super::client::Client;
use super::event_loop::{Event, EventLoop, FileRequest, FileResponse};
use crate::state::{GlobalState, STATE};
use super::command::Command;

// a macro that is defined by libp2p called network behaviour
#[derive(NetworkBehaviour)]
pub struct ChatBehaviour {
    // what is chat behaviour event then?
    // chat behaviour event gets added automatically (the compiler makes it!)
    pub mdns: mdns::tokio::Behaviour,
    pub gossipsub: gossipsub::Behaviour,
    pub request_response: request_response::cbor::Behaviour<FileRequest, FileResponse>,
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

    swarm
        .behaviour_mut()
        .kademlia
        .set_mode(Some(kad::Mode::Server));
    swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;

    println!("Please enter a nickname:");
    let mut stdin: io::Lines<io::BufReader<io::Stdin>> = io::BufReader::new(io::stdin()).lines();

    {
        let mut new_nickname = stdin.next_line().await.unwrap().unwrap();
        new_nickname = new_nickname.trim_start().trim_end().to_string();

        let mut state = STATE.lock().unwrap();
        state.nickname = new_nickname.clone();

        println!(
            "Welcome to SwapBytes, {}! Please enter chat messages one line at a time or type \\help for a list of available commands.",
            state.nickname
        );

        let peer_id = swarm.local_peer_id().clone();
        state.nicknames.insert(peer_id, new_nickname.clone());

        // gossipsub
        let topic = gossipsub::IdentTopic::new("global-chat");
        //println!("Welcome to the global chat room! Here your messages will be propogated to all peers in the network.");
        swarm.behaviour_mut().gossipsub.subscribe(&topic)?;
        state.current_room = "global-chat".to_string();
        state.rooms.push("global-chat".to_string());
    }

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