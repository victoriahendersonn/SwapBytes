// use futures::{channel::{mpsc, oneshot}, SinkExt, StreamExt};
// use libp2p::{
//     core::Multiaddr, gossipsub, kad::{self, store::RecordStore}, mdns, multiaddr::Protocol, request_response::{self, OutboundRequestId, ResponseChannel}, swarm::{Swarm, SwarmEvent}, PeerId
// };
// use std::collections::{hash_map, HashMap, HashSet};
// use std::error::Error;

// use crate::network::behaviour::{ChatBehaviour, ChatBehaviourEvent, FileResponse, FileRequest};
// use crate::network::event::Event;

// /**
//  * 
//  */
// #[derive(Debug)]
// pub enum Command {
//     DirectMessage {
//         peer_id: PeerId,
//         peer_addr: Multiaddr,
//     },
//     SendMessage {
//         room: String,
//         message: String,
//     },
//     CreateRoom {
//         room: String,
//     },
//     GetRooms{},
//     RequestFile {
//         file_name: String,
//         peer: PeerId,
//     },
//     ResponseFile {
//         file: Vec<u8>,
//         channel: ResponseChannel<FileResponse>,
//     },
// }

// pub struct EventLoop {
//     swarm: Swarm<ChatBehaviour>,
//     command_receiver: mpsc::Receiver<Command>,
//     event_sender: mpsc::Sender<Event>,
//     pending_dial: HashMap<PeerId, oneshot::Sender<Result<(), Box<dyn Error + Send>>>>,
//     pending_start_providing: HashMap<kad::QueryId, oneshot::Sender<()>>,
//     pending_get_providers: HashMap<kad::QueryId, oneshot::Sender<HashSet<PeerId>>>,
//     pending_request_file: HashMap<OutboundRequestId, oneshot::Sender<Result<Vec<u8>, Box<dyn Error + Send>>>>,
// }

// impl EventLoop {
//     pub fn new(
//         swarm: Swarm<ChatBehaviour>,
//         command_receiver: mpsc::Receiver<Command>,
//         event_sender: mpsc::Sender<Event>,
//     ) -> Self {
//         Self {
//             swarm,
//             command_receiver,
//             event_sender,
//             pending_dial: Default::default(),
//             pending_start_providing: Default::default(),
//             pending_get_providers: Default::default(),
//             pending_request_file: Default::default(),
//         }
//     }

//     /**
//      * 
//      */
//     pub async fn run(mut self) {
//         loop {
//             tokio::select! {
//                 event = self.swarm.select_next_some() => { self.handle_event(event).await },
//                 command = self.command_receiver.next() => match command {
//                     Some(c) => self.handle_command(c).await,
//                     None => return,
//                 },
//             }
//         }
//     }

//     /**
//      * 
//      */
//     pub async fn handle_event(&mut self, event: SwarmEvent<ChatBehaviourEvent>) {
//         match event {
//             SwarmEvent::NewListenAddr { address, .. } => {
//                 let local_peer_id = *self.swarm.local_peer_id();
//                 eprintln!("Listening on {:?}", address.with(Protocol::P2p(local_peer_id)));
//             }

//             // Event handling for mDNS events...
//             SwarmEvent::Behaviour(ChatBehaviourEvent::Mdns(event)) => match event {
//                 mdns::Event::Discovered(peers) => {
//                     for (peer_id, _addr) in peers {
//                         println!("mDNS discovered a peer: {:?}", peer_id);
//                         self.swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
//                     }
//                 }
//                 mdns::Event::Expired(peers) => {
//                     for (peer_id, _addr) in peers {
//                         println!("mDNS peer expired: {:?}", peer_id);
//                         self.swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
//                     }
//                 }
//             },

//             // Event handling for GossipSub...
//             SwarmEvent::Behaviour(ChatBehaviourEvent::Gossipsub(event)) => match event {
//                 gossipsub::Event::Message { message, .. } => {
//                     let msg = String::from_utf8_lossy(&message.data);
//                     println!("Received Gossipsub message: {}", msg);
//                 }
//                 gossipsub::Event::Subscribed { peer_id, .. } => {
//                     println!("Peer subscribed to our topic: {:?}", peer_id);
//                 }
//                 gossipsub::Event::Unsubscribed { peer_id, .. } => {
//                     println!("Peer unsubscribed from our topic: {:?}", peer_id);
//                 }
//                 _ => {}
//             },

//             // Event handling for Kademlia...
//             SwarmEvent::Behaviour(ChatBehaviourEvent::Kademlia(
//                 kad::Event::OutboundQueryProgressed {
//                     id,
//                     result,
//                     ..
//                 },
//             )) => match result {
//                 kad::QueryResult::StartProviding(_) => {
//                     let sender: oneshot::Sender<()> = self
//                     .pending_start_providing
//                     .remove(&id)
//                     .expect("Completed query to be previously pending.");
//                     let _ = sender.send(());
//                 }
//                 kad::QueryResult::GetProviders(Ok(kad::GetProvidersOk::FoundProviders {
//                     providers,
//                     ..
//                 })) => {
//                     if let Some(sender) = self.pending_get_providers.remove(&id) {
//                         sender.send(providers).expect("Receiver not to be dropped");
    
//                         // Finish the query. We are only interested in the first result.
//                         self.swarm
//                             .behaviour_mut()
//                             .kademlia
//                             .query_mut(&id)
//                             .unwrap()
//                             .finish();
//                     }
//                 }
//                 kad::QueryResult::GetProviders(Ok(
//                     kad::GetProvidersOk::FinishedWithNoAdditionalRecord { .. },
//                 )) => {}
//                 _ => {}
//             }
            
//             // Event handling for RequestResponse...
//             SwarmEvent::Behaviour(ChatBehaviourEvent::RequestResponse(
//                 request_response::Event::Message { message, .. },
//             )) => match message {
//                 request_response::Message::Request {
//                     request, channel, ..
//                 } => {
//                     self.event_sender
//                         .send(Event::InboundRequest {
//                             request: request.0,
//                             channel,
//                         })
//                         .await
//                         .expect("Event receiver not to be dropped.");
//                 }
//                 request_response::Message::Response {
//                     request_id,
//                     response,
//                 } => {
//                     let _ = self
//                         .pending_request_file
//                         .remove(&request_id)
//                         .expect("Request to still be pending.")
//                         .send(Ok(response.0));
//                 }
//             },

//             SwarmEvent::Behaviour(ChatBehaviourEvent::RequestResponse(
//                 request_response::Event::OutboundFailure {
//                     request_id, error, ..
//                 },
//             )) => {
//                 let _ = self
//                     .pending_request_file
//                     .remove(&request_id)
//                     .expect("Request to still be pending.")
//                     .send(Err(Box::new(error)));
//             }
            
//             SwarmEvent::Behaviour(ChatBehaviourEvent::RequestResponse(
//                 request_response::Event::ResponseSent { .. },
//             )) => {}

//             SwarmEvent::IncomingConnection { .. } => {}

//             SwarmEvent::ConnectionEstablished {
//                 peer_id, endpoint, ..
//             } => {
//                 if endpoint.is_dialer() {
//                     if let Some(sender) = self.pending_dial.remove(&peer_id) {
//                         let _ = sender.send(Ok(()));
//                     }
//                 }
//             }

//             SwarmEvent::ConnectionClosed { .. } => {}

//             SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
//                 if let Some(peer_id) = peer_id {
//                     if let Some(sender) = self.pending_dial.remove(&peer_id) {
//                         let _ = sender.send(Err(Box::new(error)));
//                     }
//                 }
//             }

//             SwarmEvent::IncomingConnectionError { .. } => {}

//             SwarmEvent::Dialing {
//                 peer_id: Some(peer_id),
//                 ..
//             } => eprintln!("Dialing {peer_id}"),
            
//             _ => {}
//         }
//     }

//     /**
//      * 
//      */
//     async fn handle_command(&mut self, command: Command) {
//         match command {
//             Command::DirectMessage { peer_id, peer_addr } => {
//                 if let hash_map::Entry::Vacant(e) = self.pending_dial.entry(peer_id) {
//                     self.swarm
//                     .behaviour_mut()
//                     .kademlia
//                     .add_address(&peer_id, peer_addr.clone());
//                 } else {
//                     todo!("Already dialing peer");
//                 }
//             }

//             Command::SendMessage { room, message } => {
//                 let topic = gossipsub::IdentTopic::new(room);
//                 if let Err(err) = self.swarm.behaviour_mut().gossipsub.publish(topic.clone(), message.as_bytes()) {
//                     println!("Error publishing message: {:?}", err)
//                 }
//             }

//             Command::CreateRoom { room } => {
//                 let key = kad::RecordKey::new(&"rooms".to_string());
//                 let record = self.swarm.behaviour_mut().kademlia.store_mut().get(&key);
                
//                 if record.is_none() {
//                     let rooms = vec![room];
//                     let rooms_bytes = serde_cbor::to_vec(&rooms).unwrap();

//                     let record = kad::Record {
//                         key: kad::RecordKey::new(&"rooms".to_string()),
//                         value: rooms_bytes,
//                         publisher: None,
//                         expires: None,
//                     };

//                     self.swarm.behaviour_mut().kademlia.put_record(record, kad::Quorum::One).expect("");
//                 } else {
//                     let mut rooms: Vec<String> = match serde_cbor::from_slice(&record.unwrap().value) {
//                         Ok(rooms) => rooms,
//                         Err(e) => {
//                             eprintln!("Failed to deserialize room list: {:?}", e);
//                             return;
//                         }
//                     };

//                     rooms.push(room.clone());

//                     let rooms_bytes = serde_cbor::to_vec(&rooms).unwrap();

//                     let record = kad::Record {
//                         key: kad::RecordKey::new(&"rooms".to_string()),
//                         value: rooms_bytes,
//                         publisher: None,
//                         expires: None,
//                     };

//                     self.swarm.behaviour_mut().kademlia.put_record(record, kad::Quorum::One).expect("");
//                 }
//             }
            
//             Command::GetRooms {} => {  
//                 self.swarm.behaviour_mut().kademlia.get_record(kad::RecordKey::new(&"rooms".to_string()));
//             }

//             Command::RequestFile { file_name, peer} => {
//                 self.swarm
//                     .behaviour_mut()
//                     .request_response
//                     .send_request(&peer, FileRequest(file_name));
//             }

//             Command::ResponseFile { file, channel } => {
//                 self.swarm
//                     .behaviour_mut()
//                     .request_response
//                     .send_response(channel, FileResponse(file))
//                     .expect("Connection to peer to be still open.");
//             }
//         }
//     }
// }



