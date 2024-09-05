use futures::{FutureExt, SinkExt, StreamExt};

use serde::{Deserialize, Serialize};
use SwapBytes::network;
use SwapBytes::state::STATE;

use network::behaviour;
use network::command::Command;
use network::event_loop::Event;
use std::{error::Error, io::Write};

use tokio::{
    io::{self, AsyncBufReadExt},
    select, spawn,
};

use color_eyre::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectMessageRequest(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectMessageResponse(pub String);

/**
 * Main function that will run the application and handle the user's input accordingly.
 */
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialise the swarm and then start the event_loop.
    let (mut client, mut event_receiver, event_loop) = behaviour::swarm().await?;
    spawn(event_loop.run());

    // Read the user's input from stdin.
    let mut stdin: io::Lines<io::BufReader<io::Stdin>> = io::BufReader::new(io::stdin()).lines();

    loop {
        select! {
            Ok(Some(line)) = stdin.next_line() => {
                if line.is_empty() {
                    println!("Please enter something!");
                    continue;
                }

                let mut command = Command::Unknown;

                if line.starts_with("/") {
                    let user_input: Vec<&str> = line.split_whitespace().collect();

                    if user_input[0] == "/start-providing" {
                        if user_input.len() < 3 || (user_input.len() > 3) {
                            println!("Usage: /start-providing <file_path> <file_name>");
                        } else {
                            client.start_providing(user_input[2].to_string()).await;

                            loop {
                                match event_receiver.next().await {
                                    // Reply with the content of the file on incoming requests.
                                    Some(Event::InboundRequest { request, channel }) => {
                                        if request == user_input[2].to_string() {
                                            client
                                                .respond_file(std::fs::read(&user_input[1].to_string())?, channel)
                                                .await;
                                        }
                                    }
                                    e => todo!("{:?}", e),
                                }
                            }
                        }
                    } else if user_input[0] == "/get-file" {
                        if user_input.len() < 2 || (user_input.len() > 3) {
                            println!("Usage: /get-file <file_name>");
                        } else {
                            let name = user_input[1].to_string();
                            // Locate all nodes providing the file.
                            let providers = client.get_providers(name.clone()).await;
                            if providers.is_empty() {
                                return Err(format!("Could not find provider for file {name}.").into());
                            }

                            // Request the content of the file from each node.
                            let requests = providers.into_iter().map(|p| {
                                let mut network_client = client.clone();
                                let name = name.clone();
                                async move { network_client.request_file(p, name).await }.boxed()
                            });

                            // Await the requests, ignore the remaining once a single one succeeds.
                            let file_content = futures::future::select_ok(requests)
                                .await
                                .map_err(|_| "None of the providers returned file.")?
                                .0;

                            println!("File contents: ");
                            std::io::stdout().write_all(&file_content)?;
                        }
                    } else if user_input[0] == "/request-file" {
                        // usage /request-file <file-name> <nickname>
                        // TODO
                        let mut state = STATE.lock().unwrap();
                        let peer_id = state.nicknames.get(libp2p::PeerId(user_input[1].to_string()));
                        let connected_peer = libp2p::PeerId(state.connected_peer);
                        if let Some(peer) = peer_id {
                            self.swarm.dial(peer).unwrap();
                        }
                        
                        let mut other_peer_id: Option<PeerId> = None;

                        if let Some(peer_id) = other_peer_id {
                            swarm.behaviour_mut().request_response.send_request(&peer_id, FileRequest(line.to_string()));
                        }
                    } else {
                        // it'll be a command, so need to create the command accodingly
                        command = create_command(&line).await;
                    }
                } else {
                    // if not a command, then it's a message to the chat that they're in...
                    // so create a message!
                    let state = STATE.lock().unwrap();
                    let message = format!("{}", line);

                    // turn the message into a command as that makes it easier to send to the client
                    command = Command::Message {
                        room: state.current_room.clone(),
                        message: message.clone(),
                    }
                }

                client.sender.send(command).await?;
            }
        }
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
}

/**
 * Creates a command based on the user's input.
 */
async fn create_command(input: &str) -> Command {
    let user_input: Vec<&str> = input.split_whitespace().collect();

    match user_input[0] {
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
        "/create-room" => {
            if user_input.len() < 2 || user_input.len() > 2 {
                println!("Usage: /create-room <room-name>");
                Command::Error
            } else {
                Command::CreateRoom { 
                    name: (user_input[1].to_string()) 
                }
            }
        }   
        "/change-room" => {
            if user_input.len() < 2 {
                println!("Usage: /change-room <room-name>");
                Command::Error
            } else {
                Command::ChangeRoom { 
                    name: (user_input[1..].join(" ")) 
                }
            }
        }
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
        "/list-files" => Command::ListFiles,
        "/list-rooms" => {
            if user_input.len() < 1 || user_input.len() > 1 {
                println!("Usage: /list-rooms");
                Command::Error
            } else {
                Command::ListRooms
            }
            
        },
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
        // "/get-providers" => {
        //     if user_input.len() < 2 || (user_input.len() > 3) {
        //         println!("Usage: /post-offer <file_name>");
        //         Command::Error
        //     } else {
        //         Command::StartProviding { file_name: (), sender: () }
        //     }
        // }
        // "/request-file" => {
        //     if user_input.len() < 2 || (user_input.len() > 3) {
        //         println!("Usage: /request-file <file_name>");
        //         Command::Error
        //     } else {
        //         Command::RequestFile {
        //             file_name: (), peer: (), sender: ()
        //         }
        //     }
        // }
        // "/response-file" => {

        // }
        "/exit" => Command::Exit,
        "/help" => Command::Help,
        _ => Command::Unknown,
    }
}
