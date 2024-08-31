use libp2p::request_response::ResponseChannel;

use crate::network::behaviour::FileResponse;

/**
 * 
 */
#[derive(Debug)]
pub enum Event {
    InboundRequest {
        request: String,
        channel: ResponseChannel<FileResponse>,
    },
}