use std::error::Error;

#[derive(NetworkBehaviour)]
struct NetBehaviour {
    gossipsub: gossipsub::Behaviour,   
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

}