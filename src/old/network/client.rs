// use futures::channel::mpsc;

// use futures::SinkExt;
// use libp2p::{
//     core::Multiaddr,
//     request_response::ResponseChannel,
//     PeerId,
// };

// use crate::network::behaviour::FileResponse;
// use crate::network::event_loop::Command;

// /**
//  * The network client that will interact with the network layer
//  * from anywhere within your application.
//  */
// #[derive(Clone)]
// pub struct Client {
//     pub sender: mpsc::Sender<Command>,
// }

// /**
//  * 
//  */
// impl Client {
//     /// Direct message the given peer at the given address.
//     pub async fn direct_message(&mut self, peer_id: PeerId, peer_addr: Multiaddr) {
//         self.sender
//             .send(Command::DirectMessage {
//                 peer_id,
//                 peer_addr,
//             })
//             .await
//             .expect("Command receiver not to be dropped.");
//     }

//      // Send a message to the specified room.
//      pub async fn send_message(&mut self, room: String, message: String) {
//         self.sender
//             .send(Command::SendMessage { room, message })
//             .await
//             .expect("Command receiver not to be dropped.");
//     }

//     // Create a new room with the given name.
//     pub async fn create_room(&mut self, room: String) {
//         self.sender
//             .send(Command::CreateRoom { room })
//             .await
//             .expect("Command receiver not to be dropped.");
//     }

//     // Request the content of the given file from the given peer.
//     pub async fn request_file(&mut self, peer: PeerId, file_name: String) {
//         self.sender
//             .send(Command::RequestFile {
//                 file_name,
//                 peer,
//             })
//             .await
//             .expect("Command receiver not to be dropped.");
//     }

//     // Respond with the provided file content to the given request.
//     pub async fn respond_file(&mut self, file: Vec<u8>, channel: ResponseChannel<FileResponse>) {
//         self.sender
//             .send(Command::ResponseFile { file, channel })
//             .await
//             .expect("Command receiver not to be dropped.");
//     }


//     pub async fn get_rooms(&mut self) {
//         self.sender
//             .send(Command::GetRooms { })
//             .await
//             .expect("Command receiver not to be dropped.");
//     }
// }