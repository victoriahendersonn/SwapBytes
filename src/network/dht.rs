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
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    loop {
        select! {
            //todo
        }
    }

}

fn split_string(input: &str) -> Vec<String> {
    let re = Regex::new(r#""([^"]*)"|\S+"#).unwrap();
    re.captures_iter(input)
        .map(|cap| cap.get(0).unwrap().as_str().to_string())
        .collect()
}
