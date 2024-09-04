use clap::Parser;
use futures::StreamExt;
use lazy_static::lazy_static;
use libp2p::kad::store::MemoryStore;
use libp2p::kad::QueryId;
use libp2p::request_response::ProtocolSupport;
use libp2p::{gossipsub, kad, mdns, noise, request_response, Multiaddr, StreamProtocol, Swarm};
use libp2p::{
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, PeerId,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Default)]
pub struct GlobalState {
    pub nickname: String,
    pub nicknames: HashMap<PeerId, String>,
    pub friends: Vec<String>,
    pub queries: HashMap<QueryId, PeerId>,
    pub current_room: String,
    pub rooms: Vec<String>,
    pub files: HashMap<String, PeerId>,
    pub dm: bool,
}

impl GlobalState {
    fn new() -> GlobalState {
        GlobalState::default()
    }
}

// allows the global state mutable and accessible safely across threads
lazy_static! {
    pub static ref STATE: Arc<Mutex<GlobalState>> = Arc::new(Mutex::new(GlobalState::new()));
}
