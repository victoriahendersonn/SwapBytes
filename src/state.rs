use lazy_static::lazy_static;
use libp2p::kad::QueryId;
use libp2p::PeerId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub struct GlobalState {
    pub peer_id: String,
    pub peers: Vec<PeerId>,

    pub nickname: String,
    pub nicknames: HashMap<PeerId, String>,

    pub friends: Vec<String>,

    pub queries: HashMap<QueryId, PeerId>,

    pub current_room: String, // Current room to message
    pub rooms: HashMap<String, Vec<String>>, // Room name -> Vec of messages
    pub subscriptions: HashMap<String, String>, // PeerID -> Room name

    pub files: HashMap<String, PeerId>,
}

impl GlobalState {
    fn new() -> GlobalState {
        GlobalState::default()
    }

    pub fn switch_room(&mut self, room_name: &String) {
        self.current_room = room_name.to_string();
        if !self.rooms.contains_key(room_name) {
            self.rooms.insert(room_name.to_string(), Vec::new());
        }
    }

    pub fn add_message_to_room(&mut self, room_name: &str, message: String) {
        if let Some(messages) = self.rooms.get_mut(room_name) {
            messages.push(message);
        }
    }
}

// allows the global state mutable and accessible safely across threads
lazy_static! {
    pub static ref STATE: Arc<Mutex<GlobalState>> = Arc::new(Mutex::new(GlobalState::new()));
}
