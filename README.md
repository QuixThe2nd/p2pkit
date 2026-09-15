
## Bootstrapping server

`bootstrapping-server/` contains a minimal find/pair/relay WebSocket server for two-peer games: clients send `{"t":"find"}`, the server FIFO-pairs them (first waiter = host), then relays opaque `{"t":"sig"}` payloads verbatim between the pair until disconnect (`peer_left`). See its README. Run: `node bootstrapping-server/server.js`.
