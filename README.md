
## Bootstrapping server

`bootstrapping-server/` contains a minimal find/pair/relay WebSocket server for two-peer games: clients send `{"t":"find"}`, the server FIFO-pairs them (first waiter = host), then relays opaque `{"t":"sig"}` payloads verbatim between the pair until disconnect (`peer_left`). It is written in TypeScript and shares its lobby wire types with the client library (`src/signalling/lobby.ts`, exported as `p2pkit/signalling`). Build it with `npm install && npm run build` in `bootstrapping-server/`, then run `node bootstrapping-server/dist/server.js`. See its README.
