use crossterm::terminal::{self, ClearType};
use futures::{FutureExt, SinkExt, StreamExt};

use serde::{Deserialize, Serialize};
use SwapBytes::network;
use SwapBytes::state::STATE;

use network::behaviour;
use network::command::Command;
use network::event_loop::Event;
use std::error::Error;

use tokio::{
    io::{self, AsyncBufReadExt, AsyncWriteExt, BufWriter},
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

                if line.trim_start().starts_with("/") {
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
                                            client.respond_file(std::fs::read(&user_input[1].to_string())?, channel).await;
                                            break;
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
                            println!("{}", std::str::from_utf8(&file_content)?);
                        }
                    } else if user_input[0] == "/request-file" {
                        // TODO
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
                    };

                    // Removes the user's standard output from the terminal screen.
                    print!("\x1b[F\x1b[K");
                    // TODO
                }

                match command {
                    Command::CreateRoom { ref name } => {
                        // Send the CreateRoom command
                        client.sender.send(Command::CreateRoom { name: name.clone() }).await?;

                        // Create the Notification command using the room name and send it everywhere.
                        let mut rooms;
                        
                        {
                            let state = STATE.lock().unwrap();
                            rooms = state.rooms.clone();
                        }

                        for (room, _messages) in rooms {
                            let message = format!("[All] SwapBytes: Room '{}' has been created, come join when you'd like to!", name);
                            client.sender.send(Command::Message { room, message }).await?;
                        }

                        // Continue to the next loop iteration
                        continue;
                    }
                    _ => client.sender.send(command).await?,
                }
            }
        }
    }
}

/**
 * Creates a command based on the user's input.
 */
async fn create_command(input: &str) -> Command {
    let user_input: Vec<&str> = input.split_whitespace().collect();

    match user_input[0] {
        "/dm" => {
            if user_input.len() < 3 {
                // TODO if the user has a long username then ... it can't find because it just assumes the rest of the user is the message
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