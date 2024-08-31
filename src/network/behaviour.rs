
use futures::channel::mpsc;
use futures::prelude::*;

use libp2p::{
    gossipsub, identity, kad, 
    mdns, noise, request_response::{self, ProtocolSupport}, 
    swarm::NetworkBehaviour, tcp, yamux, Multiaddr, 
    StreamProtocol
};

use std::{error::Error, time::Duration};
use serde::{Deserialize, Serialize};
use clap::Parser;

use crate::network::client::Client;
use crate::network::event_loop::EventLoop;
use crate::network::event::Event;

/**
 * Determines the behaviour of the chat.
 */
#[derive(NetworkBehaviour)]
pub struct ChatBehaviour {
    //pub mdns: mdns::tokio::Behaviour,
    //pub gossipsub: gossipsub::Behaviour,
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
 * Command Line Parsing.
 */
#[derive(Parser, Debug)]
#[clap(name = "libp2p request response example")]
pub struct Opt {
    #[clap(long)]
    port: Option<String>,

    #[clap(long)]
    peer: Option<Multiaddr>,
}


/**
 * Builds a Swarm, which will contain the state of the network as a whole - all of the 
 * behaviour of a libp2p network can be controlled through the swarm.
 * 
 * It will contain all active and pending connections to remotes and manages the state 
 * of all the substreams that have been opened, along with all the upgrades that were
 * built upon these substreams.
 */
pub async fn swarm(
    secret_key_seed: Option<u8>,
) -> Result<(Client, impl Stream<Item = Event>, EventLoop), Box<dyn Error>> {
    // creating a public/private key pair
    let id_keys = match secret_key_seed {
        Some(seed) => {
            let mut bytes = [0u8; 32];
            bytes[0] = seed;
            identity::Keypair::ed25519_from_bytes(bytes).unwrap()
        }
        None => identity::Keypair::generate_ed25519(),
    };

    // users peer id.
    let peer_id = id_keys.public().to_peer_id();

    // now to create the swarm
    // let mut swarm = libp2p::SwarmBuilder::with_existing_identity(id_keys)
    //     .with_tokio()
    //     .with_tcp(
    //         tcp::Config::default(), 
    //         noise::Config::new, 
    //         yamux::Config::default)?
    //     .with_quic()
    //     .with_behaviour(|key| {
    //         Ok(ChatBehaviour{
    //             mdns: mdns::tokio::Behaviour::new(
    //                 mdns::Config::default(), 
    //                 peer_id
    //             )?,
    //             gossipsub: gossipsub::Behaviour::new(
    //                 gossipsub::MessageAuthenticity::Signed(key.clone()), 
    //                 gossipsub::Config::default(),
    //             )?,
    //             request_response: request_response::cbor::Behaviour::new(
    //                 [(
    //                     StreamProtocol::new("/file-exchange/1"),
    //                     ProtocolSupport::Full,
    //                 )],
    //                 request_response::Config::default(),
    //             ),
    //             kademlia: kad::Behaviour::new(
    //                 peer_id,
    //                 // TODO does this need to change?
    //                 kad::store::MemoryStore::new(key.public().to_peer_id()),
    //             )
                
    //         })
    //     })?
    //     .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
    //     .build();

    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(id_keys)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|key| ChatBehaviour {
            kademlia: kad::Behaviour::new(
                peer_id,
                kad::store::MemoryStore::new(key.public().to_peer_id()),
            ),
            request_response: request_response::cbor::Behaviour::new(
                [(
                    StreamProtocol::new("/file-exchange/1"),
                    ProtocolSupport::Full,
                )],
                request_response::Config::default(),
            ),
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();


    swarm
        .behaviour_mut()
        .kademlia
        .set_mode(Some(kad::Mode::Server));

    let (command_sender, command_receiver) = mpsc::channel(0);
    let (event_sender, event_receiver) = mpsc::channel(0);

    Ok((
        Client {
            sender: command_sender,
        },
        event_receiver,
        EventLoop::new(swarm, command_receiver, event_sender),
    ))

    // // gossipsub will publish messages in topics
    // let topic = gossipsub::IdentTopic::new("global-chat");
    // swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    // // swarm listening in using a multiaddr format, listening in on both TCP and
    // // QUIC.
    // swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
    // swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;


    // FROM TUTORIAL
    // let mut stdin = io::BufReader::new(io::stdin()).lines();
    // println!("Enter chat messages one line at a time:");

    // loop {
    //     select! {
    //         Ok(Some(line)) = stdin.next_line() => {
    //             if let Err(err) = swarm.behaviour_mut().gossipsub.publish(topic.clone(), line.as_bytes()) {
    //                 println!("Publish error: {err:?}");
    //             }
    //         }
    //         event = swarm.select_next_some() => match event {
    //             SwarmEvent::NewListenAddr { address, .. } => println!("Your node is listening on {address}"),
    //             _ => {}
    //             SwarmEvent::Behaviour(ChatBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
    //                 for (peer_id, multiaddr) in list {
    //                     println!("mDNS discovered a new peer: {peer_id}, listening on {multiaddr}");
    //                     swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
    //                 }
    //             }
    //             SwarmEvent::Behaviour(ChatBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
    //                 for (peer_id, multiaddr) in list {
    //                     println!("mDNS peer has expired: {peer_id}, listening on {multiaddr}");
    //                     swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
    //                 }
    //             }
    //             SwarmEvent::Behaviour(ChatBehaviourEvent::Gossipsub(gossipsub::Event::Message {
    //                 propagation_source: peer_id,
    //                 message_id: id,
    //                 message,
    //             })) => println!(
    //                     "Got message: '{}' with id: {id} from peer: {peer_id}",
    //                     String::from_utf8_lossy(&message.data),
    //             ),
    //         }
    //     }
    // }
}
