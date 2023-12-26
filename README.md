# DB to IPFS service

### How to run
1. Install ipfs cli tool via given [link](https://docs.ipfs.tech/install/command-line/#install-official-binary-distributions) for your platform. Arm systems are not supported at the moment.  
2. Run theese commands in cli
```bash
    ipfs init
    ipfs daemon
```
These commands will initialize you local ipfs node and run it.  
3. Create you own .env file in the project root, add necessary variables (see .env.example)  
4. Run the application
```bash
    cargo build --release
    cargo run
```
