use futures::channel::oneshot;

use libp2p::{
    core::Multiaddr,
    request_response::ResponseChannel,
    PeerId,
};

use std::collections::HashSet;
use std::error::Error;

use super::event_loop::FileResponse;

#[derive(Debug)]
pub enum Command {
    StartProviding {
        file_name: String,
        sender: oneshot::Sender<()>,
    },
    GetProviders {
        file_name: String,
        sender: oneshot::Sender<HashSet<PeerId>>,
    },
    RequestFile {
        file_name: String,
        peer: PeerId,
        sender: oneshot::Sender<Result<Vec<u8>, Box<dyn Error + Send>>>,
    },
    RespondFile {
        file: Vec<u8>,
        channel: ResponseChannel<FileResponse>,
    },
    Message {
        room: String,
        message: String,
    },
    DirectMessage {
        peer_nickname: String,
        message: String,
    },
    TradeRequest {
        peer_nickname: String,
        message: Option<String>,
    },
    TradeResponse {
        peer_nickname: String,
        message: Option<String>,
    },
    ListFiles,
    ListPeers,
    SetNickname {
        new_nickname: String,
    },
    CreateRoom,
    ChangeRoom,
    ListRooms,
    Exit,
    Help,
    Unknown,
    Error,
}