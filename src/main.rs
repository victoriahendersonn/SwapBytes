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
use SwapBytes::network;
use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::{
    io::{self, AsyncBufReadExt},
    select, spawn,
};

use color_eyre::Result;
use ratatui::{
    layout::{Constraint, Layout, Position},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span, Text},
    widgets::{Block, List, ListItem, Paragraph},
    Frame,
}; 

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

#[derive(Parser, Debug)]
#[clap(name = "SwapBytes application")]
struct Opt {
    #[clap(long)]
    peer: Option<Multiaddr>,

    #[clap(long)]
    listen_address: Option<Multiaddr>,

    #[clap(subcommand)]
    argument: Option<CliArgument>,
}

#[derive(Debug, Parser)]
enum CliArgument {
    Provide {
        #[clap(long)]
        path: PathBuf,
        #[clap(long)]
        name: String,
    },
    Get {
        #[clap(long)]
        name: String,
    },
}

pub enum State {
    GlobalChat,
    DirectMessage,
    Chat,
}

// Implement the Default trait for State
impl Default for State {
    fn default() -> Self {
        State::GlobalChat // Set the default variant to GlobalChat
    }
}

use network::behaviour;

#[derive(Default)]
pub struct GlobalState {
    pub nickname: String,
    pub nicknames: HashMap<PeerId, String>,
    pub friends: Vec<String>,
    pub state: State,
    pub queries: HashMap<QueryId, PeerId>,
    pub current_room: String,
    pub rooms: Vec<String>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectMessageRequest(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectMessageResponse(pub String);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // swarm
    let (mut client, mut _event_receiver, event_loop) = behaviour::swarm().await?;

    // start event loop
    spawn(event_loop.run());


    // working swarm!
    // let mut swarm = libp2p::SwarmBuilder::with_new_identity()
    //     .with_tokio()
    //     .with_tcp(
    //         tcp::Config::default(),
    //         noise::Config::new,
    //         yamux::Config::default,
    //     )? // how are we going to send the bits back and forth across our network?
    //     .with_quic() // will upgrade from TCP to QUIC if it can...
    //     .with_behaviour(|key| {
    //         Ok(ChatBehaviour {
    //             mdns: mdns::tokio::Behaviour::new(
    //                 mdns::Config::default(),
    //                 key.public().to_peer_id(),
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
    //                 key.public().to_peer_id(),
    //                 MemoryStore::new(key.public().to_peer_id()),
    //             ),
    //         })
    //     })?
    //     .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
    //     .build();

    // swarm.behaviour_mut().kademlia.set_mode(Some(kad::Mode::Server));
    // swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;

    println!("Please enter a nickname:");
    let mut stdin: io::Lines<io::BufReader<io::Stdin>> = io::BufReader::new(io::stdin()).lines();

    {
        let new_nickname = stdin.next_line().await.unwrap().unwrap();

        let mut state = STATE.lock().unwrap();
        state.nickname = new_nickname.clone();

        println!(
            "Welcome to SwapBytes, {}! Please enter chat messages one line at a time.",
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

    // color_eyre::install()?;
    // let mut terminal: ratatui::Terminal<ratatui::prelude::CrosstermBackend<std::io::Stdout>> = ratatui::init();
    // let mut app_result = App::new();

    // loop {
    //     terminal.draw(|frame| app_result.draw(frame))?;

    //     if let Event::Key(key) = event::read()? {
    //         match app_result.input_mode {
    //             InputMode::Normal => match key.code {
    //                 KeyCode::Char('e') => {
    //                     app_result.input_mode = InputMode::Editing;
    //                 }
    //                 KeyCode::Char('q') => {
    //                     return Ok(());
    //                 }
    //                 _ => {}
    //             },
    //             InputMode::Editing if key.kind == KeyEventKind::Press => match key.code {
    //                 KeyCode::Enter => app_result.submit_message(),
    //                 KeyCode::Char(to_insert) => app_result.enter_char(to_insert),
    //                 KeyCode::Backspace => app_result.delete_char(),
    //                 KeyCode::Left => app_result.move_cursor_left(),
    //                 KeyCode::Right => app_result.move_cursor_right(),
    //                 KeyCode::Esc => app_result.input_mode = InputMode::Normal,
    //                 _ => {}
    //             },
    //             InputMode::Editing => {}
    //         }
    //     }
    // }

    // ratatui::restore();

    loop {
        select! {
            Ok(Some(line)) = stdin.next_line() => {
                let mut command = Command::Unknown;
                if line.starts_with("/") {
                    command = create_command(&line).await;
                } else {
                    // if not a command, then it's a message to the chat that they're in..,
                    // create the message!
                    let mut state = STATE.lock().unwrap();
                    let message = format!("{}", line);

                    command = Command::Message {
                        room: state.current_room.clone(),
                        message: message.clone(),
                    };
                }
                handle_command(&mut swarm, command);
            }
            event = swarm.select_next_some() => match event {
                SwarmEvent::Behaviour(ChatBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, multiaddr) in list {
                        //println!("Discovered peer: {} at {}", peer_id, multiaddr);
                        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                        swarm.behaviour_mut().kademlia.add_address(&peer_id, multiaddr);

                        // fetching the nickname from kademlia
                        let key_string = peer_id.to_string();
                        let key = kad::RecordKey::new(&key_string);
                        let query_id = swarm.behaviour_mut().kademlia.get_record(key);
                        let mut state = STATE.lock().unwrap();
                        state.queries.insert(query_id, peer_id);


                        // Create DM topic for the peers, though it will not be available unless they’ve accepted a trade
                        let local_peer_id = swarm.local_peer_id().clone();
                        let ids = vec![local_peer_id.to_base58(), peer_id.to_base58()];
                        let mut sorted_ids = ids;
                        sorted_ids.sort(); // Sort IDs alphabetically

                        // Create a topic string using a separator to ensure valid topic names
                        let topic = format!("/dm/{}", sorted_ids.join("_")); // Using underscore as a separator
                        let topic_id = gossipsub::IdentTopic::new(&topic.to_string());

                        // Subscribe to the DM topic
                        if let Err(e) = swarm.behaviour_mut().gossipsub.subscribe(&topic_id) {
                            eprintln!("Error subscribing to topic '{}': {:?}", topic_id, e);
                        }
                    }
                },
                SwarmEvent::Behaviour(ChatBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                    for (peer_id, multiaddr) in list {
                        //println!("mDNS discover peer has expired: {peer_id}");
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                        swarm.behaviour_mut().kademlia.remove_address(&peer_id, &multiaddr);
                    }
                },
                SwarmEvent::Behaviour(ChatBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source: peer_id,
                    message_id: _id,
                    message,
                })) => {
                    let state = STATE.lock().unwrap();
                    let peer_nickname = state.nicknames.get(&peer_id);

                    if peer_nickname.is_none() {
                        println!("Unkown: {}", String::from_utf8_lossy(&message.data));
                    } else {
                        let message = String::from_utf8_lossy(&message.data);
                        if message.clone().starts_with("/dm") {
                            let message_stripped = message.strip_prefix("/dm").unwrap_or(&message);
                            println!("[From] {}: {}", peer_nickname.unwrap(), message_stripped);
                        } else if state.current_room == "global-chat" {
                            println!("[Global Chat] {}: {}", peer_nickname.unwrap(), message.clone());
                        } else {
                            println!("[{}] {}: {}", state.current_room, peer_nickname.unwrap(), message.clone());
                        }
                    }
                },
                SwarmEvent::NewListenAddr { address, .. } => {
                    //println!("Local node is listening on {address}");
                    if address.to_string().contains("/ip4/127.0.0.1/udp") {

                        let peer_id = swarm.local_peer_id().clone();
                        swarm.behaviour_mut().kademlia.add_address(&peer_id, address);

                        let state = STATE.lock().unwrap();
                        let nickname_bytes = serde_cbor::to_vec(&state.nickname).unwrap();
                        let key: String = peer_id.to_string();

                        let record = kad::Record {
                            key: kad::RecordKey::new(&key),
                            value: nickname_bytes,
                            publisher: None,
                            expires: None,
                        };

                        swarm.behaviour_mut().kademlia
                            .put_record(record, kad::Quorum::One)
                            .expect("");
                    }
                }

                // Kademlia events!
                SwarmEvent::Behaviour(ChatBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                    id,
                    result,
                    ..
                })) => {
                    match result {
                        // Get record return result
                        kad::QueryResult::GetRecord(Ok(
                            kad::GetRecordOk::FoundRecord(kad::PeerRecord {
                                record: kad::Record { key, value, ..},
                                ..
                            })
                        )) => {
                            match serde_cbor::from_slice::<String>(&value) {
                                Ok(nickname) => {
                                    let mut state = STATE.lock().unwrap();
                                    if state.queries.contains_key(&id) {
                                        let peer_id = state.queries.remove(&id).expect("Message was not in queue");
                                        state.nicknames.insert(peer_id.clone(), nickname.clone());
                                        println!("Added peer {} with nickname {}", peer_id, nickname); // Add this line
                                    }
                                }
                                Err(e) => {
                                    println!("Failed to decode nickname: {}", e); // Optional: Add this line to check decoding errors
                                }
                            }
                        }

                        kad::QueryResult::GetRecord(Ok(_)) => {}

                        kad::QueryResult::GetRecord(Err(err)) => {
                            println!("Failed to get record {:?}, error: {:?}", id, err);
                        }

                        kad::QueryResult::PutRecord(Ok(_)) => {
                            println!("Successfully put record {:?}", id);
                        }

                        kad::QueryResult::PutRecord(Err(err)) => {
                            // ("Failed to put record {:?}, error: {:?}", id, err);
                        }

                        _ => {}
                    }
                }
                _ => {}
            }
        }
    }
}

/**
 *
 */
pub enum Command {
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
    PostOffer {
        file_name: String,
        sender: String,
    },
    RequestFile,
    AcceptFile,
    CancelFile,
    Connect,
    CreateRoom,
    ChangeRoom,
    ListRooms,
    Exit,
    Help,
    Unknown,
    Error,
}

/**
 * Creates a command based on the user's input.
 */
async fn create_command(input: &str) -> Command {
    let user_input: Vec<&str> = input.split_whitespace().collect();

    match user_input[0] {
        "/trade" => {
            if user_input.len() < 2 || user_input.len() > 3 {
                Command::Error
            } else {
                Command::TradeRequest {
                    peer_nickname: user_input[1].to_string(),
                    message: Some(user_input[2..].join(" ")),
                }
            }
        }
        "/dm" => {
            if user_input.len() < 3 {
                println!("Usage: /dm <peer_nickname> <message>");
                Command::Error
            } else {
                Command::DirectMessage {
                    peer_nickname: user_input[1].to_string(),
                    message: user_input[2..].join(" "),
                }
            }
        }
        "/list-files" => Command::ListFiles,
        "/list-peers" => {
            if (user_input.len() < 1) || (user_input.len() > 1) {
                println!("Usage: /list-peers");
                Command::Error
            } else {
                Command::ListPeers
            }
        }
        "/set-nickname" => {
            if (user_input.len() < 2) || (user_input.len() > 2) {
                println!("Usage: /set-nickname <new_nickname>");
                Command::Error
            } else {
                Command::SetNickname {
                    new_nickname: (user_input[1].to_string()),
                }
            }
        }
        "/post-offer" => {
            if user_input.len() < 2 || (user_input.len() > 3) {
                println!("Usage: /post-offer <file_name>");
                Command::Error
            } else {
                Command::PostOffer {
                    file_name: user_input[1].to_string(),
                    sender: user_input[2].to_string(),
                }
            }
        },
        "/request-file" => Command::RequestFile,
        "/accept-file" => Command::AcceptFile,
        "/cancel-file" => Command::CancelFile,
        "/connect" => Command::Connect,
        "/exit" => Command::Exit,
        "/help" => Command::Help,
        _ => Command::Unknown,
    }
}

pub fn handle_command(swarm: &mut Swarm<ChatBehaviour>, command: Command) {
    // lock the state at the beginning to ensure a consistent state!
    let mut state: MutexGuard<GlobalState> = STATE.lock().unwrap();

    // then handle the command that was passed in.
    match command {
        Command::Error => {
            println!("Please use the command correctly!")
        }

        Command::DirectMessage {
            peer_nickname,
            message,
        } => {
            // if !state.friends.contains(&peer_nickname) {
            //     println!("You are not able to direct message {}, please try /trade as they need to accept your response.", peer_nickname);
            // } else  {
            // the current user's peer id.
            let local_peer_id = swarm.local_peer_id().clone();

            // find the peer ID associated with the given nickname.
            let peer_id = state.nicknames.iter().find_map(|(id, nickname)| {
                if nickname == &peer_nickname {
                    Some(id.clone())
                } else {
                    None
                }
            });

            // if this peer exists, send the message to the direct message that they are both subscribed to.
            if let Some(peer_id) = peer_id {
                // create the message to send: the user's nickname and the message
                let local_peer_id = swarm.local_peer_id().clone();
                let ids = vec![local_peer_id.to_base58(), peer_id.to_base58()];
                let mut sorted_ids = ids;
                sorted_ids.sort(); // Sort IDs alphabetically

                // Create a topic string using a separator to ensure valid topic names
                let topic = format!("/dm/{}", sorted_ids.join("_")); // Using underscore as a separator
                let topic_id = gossipsub::IdentTopic::new(&topic.to_string());

                let data = ("/dm".to_string() + &message).as_bytes().to_vec();
                swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(topic_id, data)
                    .expect("Failed to publish direct message.");

                println!("[To] {}: {}", peer_nickname, message);
            } else {
                println!("Peer with nickname {} not found.", peer_nickname);
            }
            //}
        }

        Command::SetNickname { new_nickname } => {
            let old_nickname = state.nickname.clone();
            state.nickname = new_nickname.clone();

            // Update the nickname in Kademlia (DHT)
            let peer_id = swarm.local_peer_id().clone();
            let key = kad::RecordKey::new(&peer_id.to_string());
            let record = kad::Record {
                key,
                value: serde_cbor::to_vec(&new_nickname).unwrap(),
                publisher: None,
                expires: None,
            };

            if let Err(e) = swarm
                .behaviour_mut()
                .kademlia
                .put_record(record, kad::Quorum::One)
            {
                eprintln!("Failed to update nickname in DHT: {:?}", e);
            } else {
                println!(
                    "Nickname changed from '{}' to '{}'",
                    old_nickname, new_nickname
                );
            }
        }

        Command::Message { message, room } => {
            // create a new topic
            let topic = gossipsub::IdentTopic::new(room);

            // Publish the message with the user's nickname prepended
            if let Err(e) = swarm
                .behaviour_mut()
                .gossipsub
                .publish(topic.clone(), message.as_bytes())
            {
                if e.to_string().contains("InsufficientPeers") {
                    if topic.clone().to_string() == "global-chat" {
                        println!("No one can hear you... [No peers subscribed to global chat]");
                        println!("[Global Chat] You: {}", message);
                    } else {
                        println!("No one can hear you... [No peers subscribed to the {} chat]", topic.to_string());
                        println!("[{}] You: {}", topic.to_string(), message);
                    }
                } else {
                    println!("Publish error: {e:?}");
                } 
            } else {
                println!("[Global Chat] You: {}", message);
            }
        }

        Command::ListPeers => {
            println!("Listing all available peers, would you like to contact one?");
            let peers = state.nicknames.clone(); // access the state safely
            for (_peer_id, nickname) in peers {
                if state.nickname == nickname {
                    continue;
                }
                println!("{}", nickname);
            }
        }

        Command::ListFiles => {
            println!("Listing all available files, with their respective owners:");
        }

        Command::PostOffer { file_name, sender } => {
            //
        }

        Command::RequestFile => {
            println!("Requesting file");
        }

        Command::AcceptFile => {
            println!("Accepting file");
        }

        Command::CancelFile => {
            println!("Cancelling file");
        }

        Command::Connect => {
            println!("Connecting");
        }

        Command::CreateRoom => {
            println!("Creating room");
        }

        Command::ChangeRoom => {
            println!("Changing room");
        }

        Command::ListRooms => {
            println!("Listing rooms");
        }

        Command::Exit => {
            println!("Exiting SwapBytes. Goodbye!");
            std::process::exit(0);
        }

        Command::TradeRequest {
            peer_nickname,
            message,
        } => {
            // the current user's peer id and their nickname!
            let local_peer_id = swarm.local_peer_id().clone();
            let local_nickname = state.nickname.clone();

            // need the peer id of the peer we want to trade with along with that we have their nickname!
            let peer_id = state.nicknames.iter().find_map(|(id, nickname)| {
                if nickname == &peer_nickname {
                    Some(id.clone())
                } else {
                    None
                }
            });

            // send a trade request?
            // send a trade response?
            // need to get a yes... for the trade...
            // how to ensure that the peer is okay with this?
            println!("Starting trade request!");
        }

        Command::TradeResponse {
            peer_nickname,
            message,
        } => {
            // the current user's peer id and their nickname!
            let local_peer_id = swarm.local_peer_id().clone();
            let local_nickname = state.nickname.clone();

            // need the peer id of the peer we want to trade with along with that we have their nickname!
            let peer_id = state.nicknames.iter().find_map(|(id, nickname)| {
                if nickname == &peer_nickname {
                    Some(id.clone())
                } else {
                    None
                }
            });

            // if message.contains("yes") {
            //     // if the message is yes, then commence create a direct msg topic!
            //     println!("Can now direct message one another??!");
            //     state.friends.push(peer_nickname.clone()); // i need to do this for both and im not sure how to..
            // } else {
            //     println!("Trade request denied, you still cannot direct message {}", peer_nickname);
            // }
            println!("Sending response to trade request!");
        }

        Command::Help => {
            // Print available commands and their usage
            println!("Available commands:");
            println!("/trade <peer_nickname> [message] - Send a trade request");
            println!("/dm <peer_nickname> <message> - Send a direct message");
            println!("/list-files - List available files");
            println!("/list-peers - List available peers");
            println!("/set-nickname <new_nickname> - Change your nickname");
            println!("/post-offer - Post a file offer");
            println!("/request-file - Request a file");
            println!("/accept-file - Accept a file offer");
            println!("/cancel-file - Cancel a file transfer");
            println!("/connect - Connect to a peer");
            println!("/create-room - Create a new chat room");
            println!("/change-room - Change the current chat room");
            println!("/list-rooms - List available chat rooms");
            println!("/exit - Exit the application");
        }

        Command::Unknown => {
            println!("Unknown command! Type /help for a list of available commands.");
        }
    }
}


/// App holds the state of the application
struct App {
    /// Current value of the input box
    input: String,
    /// Position of cursor in the editor area.
    character_index: usize,
    /// Current input mode
    input_mode: InputMode,
    /// History of recorded messages
    messages: Vec<String>,
}

enum InputMode {
    Normal,
    Editing,
}

impl App {
    const fn new() -> Self {
        Self {
            input: String::new(),
            input_mode: InputMode::Normal,
            messages: Vec::new(),
            character_index: 0,
        }
    }

    fn move_cursor_left(&mut self) {
        let cursor_moved_left = self.character_index.saturating_sub(1);
        self.character_index = self.clamp_cursor(cursor_moved_left);
    }

    fn move_cursor_right(&mut self) {
        let cursor_moved_right = self.character_index.saturating_add(1);
        self.character_index = self.clamp_cursor(cursor_moved_right);
    }

    fn enter_char(&mut self, new_char: char) {
        let index = self.byte_index();
        self.input.insert(index, new_char);
        self.move_cursor_right();
    }

    /// Returns the byte index based on the character position.
    ///
    /// Since each character in a string can be contain multiple bytes, it's necessary to calculate
    /// the byte index based on the index of the character.
    fn byte_index(&self) -> usize {
        self.input
            .char_indices()
            .map(|(i, _)| i)
            .nth(self.character_index)
            .unwrap_or(self.input.len())
    }

    fn delete_char(&mut self) {
        let is_not_cursor_leftmost = self.character_index != 0;
        if is_not_cursor_leftmost {
            // Method "remove" is not used on the saved text for deleting the selected char.
            // Reason: Using remove on String works on bytes instead of the chars.
            // Using remove would require special care because of char boundaries.

            let current_index = self.character_index;
            let from_left_to_current_index = current_index - 1;

            // Getting all characters before the selected character.
            let before_char_to_delete = self.input.chars().take(from_left_to_current_index);
            // Getting all characters after selected character.
            let after_char_to_delete = self.input.chars().skip(current_index);

            // Put all characters together except the selected one.
            // By leaving the selected one out, it is forgotten and therefore deleted.
            self.input = before_char_to_delete.chain(after_char_to_delete).collect();
            self.move_cursor_left();
        }
    }

    fn clamp_cursor(&self, new_cursor_pos: usize) -> usize {
        new_cursor_pos.clamp(0, self.input.chars().count())
    }

    fn reset_cursor(&mut self) {
        self.character_index = 0;
    }

    fn submit_message(&mut self) {
        self.messages.push(self.input.clone());
        self.input.clear();
        self.reset_cursor();
    }

    fn draw(&self, frame: &mut Frame) {
        let vertical = Layout::vertical([
            Constraint::Min(1),
            Constraint::Length(3),
            Constraint::Length(1),
        ]);
        let [messages_area, input_area, help_area] = vertical.areas(frame.area());
    
        let messages: Vec<ListItem> = self
            .messages
            .iter()
            .enumerate()
            .map(|(i, m)| {
                let content = Line::from(Span::raw(format!("{i}: {m}")));
                ListItem::new(content)
            })
            .collect();
        let messages = List::new(messages).block(Block::bordered().title("Messages"));
        frame.render_widget(messages, messages_area);
    
        let input = Paragraph::new(self.input.as_str())
            .style(match self.input_mode {
                InputMode::Normal => Style::default(),
                InputMode::Editing => Style::default().fg(Color::Yellow),
            })
            .block(Block::bordered().title("Input"));
        frame.render_widget(input, input_area);
    
        match self.input_mode {
            // Hide the cursor. `Frame` does this by default, so we don't need to do anything here
            InputMode::Normal => {}
    
            // Make the cursor visible and ask ratatui to put it at the specified coordinates after
            // rendering
            #[allow(clippy::cast_possible_truncation)]
            InputMode::Editing => frame.set_cursor_position(Position::new(
                // Draw the cursor at the current position in the input field.
                // This position can be controlled via the left and right arrow key
                input_area.x + self.character_index as u16 + 1,
                // Move one line down, from the border to the input line
                input_area.y + 1,
            )),
        }
    
        let (msg, style) = match self.input_mode {
            InputMode::Normal => (
                vec![
                    "Press ".into(),
                    "q".bold(),
                    " to exit, ".into(),
                    "e".bold(),
                    " to start editing.".bold(),
                ],
                Style::default().add_modifier(Modifier::RAPID_BLINK),
            ),
            InputMode::Editing => (
                vec![
                    "Press ".into(),
                    "Esc".bold(),
                    " to stop editing, ".into(),
                    "Enter".bold(),
                    " to record the message".into(),
                ],
                Style::default(),
            ),
        };
        let text = Text::from(Line::from(msg)).patch_style(style);
        let help_message = Paragraph::new(text);
        frame.render_widget(help_message, help_area);
    }
}