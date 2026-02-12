import { createServer, IncomingMessage } from 'http';
import * as decoding from 'lib0/decoding';
import * as encoding from 'lib0/encoding';
import type { Duplex } from 'stream';
import { WebSocket, WebSocketServer } from 'ws';
import { writePermissionDenied } from 'y-protocols/auth';
import * as awareness from 'y-protocols/awareness';
import * as sync from 'y-protocols/sync';
import { Doc as YDoc } from 'yjs';
import { Kart } from '../entry-node.ts';

const MESSAGE_TYPE_SYNC = 0;
const MESSAGE_TYPE_AWARENESS = 1;
const MESSAGE_TYPE_AUTH = 2;
const MESSAGE_TYPE_SELECT = 102;
const MESSAGE_TYPE_SELECT_RESPONSE = 103;

const SYNC_STEP_2 = 1;
const SYNC_TYPE_UPDATE = 2;

interface KartWebSocketParameters {
  kart:
    | {
        url: Parameters<typeof Kart.pull>[0];
        defaultRoomName: Parameters<typeof Kart.pull>[1];
        directory?: Parameters<typeof Kart.pull>[2];
        options?: Parameters<typeof Kart.pull>[3];
      }
    | Kart
    | Promise<Kart>;
  /**
   * This fires when a client attempts to convert their HTTP connection to a WebSocket connection.
   *
   * You may check whether the client is authorized to connect. If not authorized, return `blocked`
   * to reject the websocket connection. If the user should only have read access, return `readonly`
   * to allow the connection but prevent them from making changes to the Y.Doc. If the user should have
   * full read and write access, return `write`. If this function is not provided, all connections
   * will be allowed with full read and write access by default.
   */
  onUpgrade?: (
    request: IncomingMessage,
    socket: Duplex,
    head: Buffer<ArrayBuffer>
  ) => 'readonly' | 'write' | 'blocked' | void;
  /**
   * An existing HTTP server. Kart will attach a WebSocket server to this HTTP server to
   * handle WebSocket upgrade requests.
   *
   * If not provided, Kart will create its own HTTP server.
   */
  server?: ReturnType<typeof createServer>;
}

export class KartWebSocketServer {
  kart: Kart;
  server: ReturnType<typeof createServer>;
  wss: WebSocketServer;
  attachedRooms = new Map<string, Kart>();

  private constructor({ kart, server, onUpgrade }: Omit<KartWebSocketParameters, 'kart'> & { kart: Kart }) {
    this.kart = kart;

    server ??= createServer((request, response) => {
      response.writeHead(200, { 'Content-Type': 'text/plain' });
      response.end('Kart WebSocket Server');
    });

    const wss = new WebSocketServer({ noServer: true });

    server.on('upgrade', (request, socket, head) => {
      const authorization = onUpgrade?.(request, socket, head) ?? 'write';
      if (authorization === 'blocked') {
        socket.write('HTTP/1.1 401 Unauthorized');
        socket.destroy();
        return;
      }

      // create a new websocket connection
      wss.handleUpgrade(request, socket, head, (ws) => {
        this.setUpWebsocketConnection(ws, request, authorization === 'readonly').catch((error) => {
          console.error('Error setting up WebSocket connection:', error);
          ws.close(1011, 'Error setting up connection');
        });

        wss.emit('connection', ws, request);
      });
    });

    this.server = server;
    this.wss = wss;
  }

  /**
   * Creates a new KartWebSocket instance.
   *
   * The `kart` parameter can be provided in three different forms:
   * 1. An already initialized `Kart` instance.
   * 2. A promise that resolves to a `Kart` instance.
   * 3. An object containing the parameters needed to initialize a `Kart` instance using `Kart.pull()`.
   *
   * This flexibility allows you to create a `KartWebSocketServer` instance in various ways depending on your application's needs.
   */
  static async create({ kart: kartInit, ...rest }: KartWebSocketParameters) {
    let instance: KartWebSocketServer;

    // handle graceful shutdown on SIGINT (e.g. Ctrl+C in terminal)
    if ('process' in globalThis) {
      console.log('\x1b[36mPress Ctrl+C to shut down the Kart WebSocket server gracefully.\x1b[0m');
      process.on('SIGINT', () => {
        console.log('\n\x1b[33mShutting down Kart WebSocket server...\x1b[0m');
        instance.server.close();
      });
    }

    if (kartInit instanceof Kart) {
      instance = new KartWebSocketServer({ kart: kartInit, ...rest });
      return instance;
    }

    if (typeof kartInit === 'object' && 'url' in kartInit && 'defaultRoomName' in kartInit) {
      const kart = await Kart.pull(
        kartInit.url,
        kartInit.defaultRoomName,
        kartInit.directory,
        kartInit.options
      );
      instance = new KartWebSocketServer({ kart, ...rest });
      return instance;
    }

    const kart = await kartInit;
    instance = new KartWebSocketServer({ kart, ...rest });
    return instance;
  }

  [Symbol.dispose]() {
    this.wss.clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.close(1001, 'Server shutting down');
      }
    });
    this.wss.close();
    this.server.close();
    this.kart.dispose();
  }

  dispose() {
    this[Symbol.dispose]();
  }

  listen(port: number, callback?: () => void) {
    this.server.listen(port, callback);
    return this;
  }

  async waitUntilServerClosed() {
    return new Promise<void>((resolve) => {
      this.server.on('close', () => resolve());
    });
  }

  get rooms() {
    const rooms = new Set<string>();
    rooms.add(this.kart.repoTree.indexName);
    return rooms as ReadonlySet<string>;
  }

  getYDoc(room: string) {
    if (room === this.kart.repoTree.indexName) {
      return this.kart.ydoc;
    }
  }

  closeConnection(doc: SharedDocument, ws: WebSocket) {
    if (doc.connections.has(ws)) {
      doc.connections.delete(ws);
    }

    ws.close(1000, 'Connection closed by server');
  }

  protected async setUpWebsocketConnection(ws: WebSocket, request: IncomingMessage, readonly: boolean) {
    const messageQueue: any[] = [];
    const handleInitialMessage = (data: any) => messageQueue.push(data);

    // attach listener immediately to capture messages that arrive before
    // we finish setting up the connection
    ws.on('message', handleInitialMessage);

    const url = new URL(request.url ?? '', 'http://localhost');

    const room = url.pathname.slice(1); // remove leading slash
    if (room.includes('/')) {
      console.warn('Invalid room name (cannot contain slashes):', room);
      ws.send('error.INVALID_ROOM_NAME');
      ws.close(4000, 'Invalid room name');
      return;
    }

    let unsharedDoc = this.getYDoc(request.url ?? '');
    if (!unsharedDoc) {
      const requestedRef = url.searchParams.get('initRef');
      if (!requestedRef) {
        console.warn('No initial ref specified for new room:', room);
        ws.send('error.NO_INIT_REF');
        ws.close(4001, 'No initial ref specified');
        return;
      }

      const kart = await this.kart.attach(room, requestedRef);
      unsharedDoc = kart.ydoc;
      this.attachedRooms.set(room, kart);
    }

    const doc = new SharedDocument(unsharedDoc);
    doc.connections.set(ws, new Set());

    // track whether the connection is still alive
    let pongReceived = true;
    const pingInterval = setInterval(() => {
      if (!pongReceived) {
        this.closeConnection(doc, ws);
        clearInterval(pingInterval);
      } else if (doc.connections.has(ws)) {
        pongReceived = false;
        try {
          ws.ping();
        } catch (e) {
          this.closeConnection(doc, ws);
          clearInterval(pingInterval);
        }
      }
    }, 10000);
    ws.on('pong', () => {
      pongReceived = true;
    });

    ws.on('close', () => {
      this.closeConnection(doc, ws);
      clearInterval(pingInterval);
    });

    ws.on('error', (error) => {
      console.error('WebSocket error:', error);
      this.closeConnection(doc, ws);
      clearInterval(pingInterval);
    });

    ws.off('message', handleInitialMessage);
    ws.on('message', (data) => {
      if (Array.isArray(data)) {
        throw new Error('Expected message data to be a Buffer but received an array');
      }

      const kart = this.attachedRooms.get(room);
      if (!kart) {
        console.warn('Received message for unattached room:', room);
        ws.send('error.ROOM_NOT_ATTACHED');
        ws.close(4002, 'Room not attached');
        return;
      }

      this.handleIncomingMessage(new Uint8Array(data), doc, ws, kart, readonly);
    });

    // flush the queued messages from before we finished setting up the connection
    for (const data of messageQueue) {
      ws.emit('message', data);
    }
  }

  protected handleIncomingMessage(
    message: Uint8Array,
    doc: SharedDocument,
    socket: WebSocket,
    kart: Kart,
    readonly: boolean
  ) {
    try {
      const decoder = decoding.createDecoder(message);
      const messageType = decoding.readVarUint(decoder); // all messages start with the type

      // block message that would modify the document if the connection is readonly
      if (readonly) {
        const sendDenied = () => {
          const encoder = encoding.createEncoder();
          encoding.writeVarUint(encoder, MESSAGE_TYPE_AUTH);
          writePermissionDenied(encoder, 'readonly connection');
          const responseMessage = encoding.toUint8Array(encoder);
          sendMessage(responseMessage, socket);
        };

        const isAwarenessMessage = messageType === MESSAGE_TYPE_AWARENESS;
        if (isAwarenessMessage) {
          return sendDenied();
        }

        const firstTwoBytes = message.slice(0, 2);
        const isSyncStep2 = firstTwoBytes[0] === MESSAGE_TYPE_SYNC && firstTwoBytes[1] === SYNC_STEP_2;
        if (isSyncStep2) {
          return sendDenied();
        }

        const isUpdateMessage = firstTwoBytes[0] === MESSAGE_TYPE_SYNC && firstTwoBytes[1] === SYNC_TYPE_UPDATE;
        if (isUpdateMessage) {
          return sendDenied();
        }
      }

      if (messageType === MESSAGE_TYPE_SYNC) {
        const encoder = encoding.createEncoder();
        encoding.writeVarUint(encoder, MESSAGE_TYPE_SYNC);

        // read the incoming sync message and prepare a response to the client with any necessary updates
        sync.readSyncMessage(decoder, encoder, doc.ydoc, socket);

        // if the encoder has more than just the message type, it means we have updates to send back
        if (encoding.length(encoder) > 1) {
          const responseMessage = encoding.toUint8Array(encoder);
          sendMessage(responseMessage, socket);
        }
      }

      if (messageType === MESSAGE_TYPE_AWARENESS) {
        awareness.applyAwarenessUpdate(doc.awareness, decoding.readVarUint8Array(decoder), socket);
      }

      if (messageType === MESSAGE_TYPE_SELECT) {
        const selection = decoding.readVarString(decoder);
        const parts = selection.split('‾‾');

        // expect selection messages to be in the format "datasetName‾‾eic0,eid1,...,eidn"
        if (parts.length !== 2) {
          console.warn('Received invalid selection message:', selection);
          return;
        }

        // retreive the requested features from the specified dataset
        // and send them back to the client as GeoJSON
        const [datasetName, encodedIds] = parts as [string, string];
        kart.data.get(datasetName).then(async (dataset) => {
          if (!dataset) {
            console.warn('Received selection message for non-existent dataset:', datasetName);
            return;
          }

          const eids = encodedIds.split(',');
          const features = await dataset.select(eids);
          const geojson = features.toGeoJSON();

          const responseEncoder = encoding.createEncoder();
          encoding.writeVarUint(responseEncoder, MESSAGE_TYPE_SELECT_RESPONSE);
          encoding.writeVarString(responseEncoder, JSON.stringify(geojson));
          sendMessage(encoding.toUint8Array(responseEncoder), socket);
        });
      }
    } catch (error) {
      console.error('Error parsing incoming message:', error);
    }
  }
}

class SharedDocument {
  ydoc: YDoc;

  /**
   * A map of each websocket connection and its associated awareness client IDs.
   */
  connections = new Map<WebSocket, Set<number>>();
  awareness: awareness.Awareness;

  constructor(ydoc: YDoc) {
    this.ydoc = ydoc;

    this.awareness = new awareness.Awareness(this.ydoc);
    this.awareness.setLocalState(null);

    // forward all awareness updates to all connected clients
    this.awareness.on(
      'update',
      (
        { added, updated, removed }: { added: number[]; updated: number[]; removed: number[] },
        origin: unknown
      ) => {
        const changedClients = [...added, ...updated, ...removed];
        const awarenessUpdate = awareness.encodeAwarenessUpdate(this.awareness, changedClients);

        // update our record of associated client IDs for this websocket
        if (origin instanceof WebSocket) {
          const trackedClients = this.connections.get(origin);
          if (trackedClients) {
            for (const clientId of added) {
              trackedClients.add(clientId);
            }
            for (const clientId of removed) {
              trackedClients.delete(clientId);
            }
          }
        }

        // broadcast the awareness update to all connected clients
        const encoder = encoding.createEncoder();
        encoding.writeVarUint(encoder, MESSAGE_TYPE_AWARENESS);
        encoding.writeVarUint8Array(encoder, awarenessUpdate);
        this.connections.forEach((clientIds, socket) => {
          sendMessage(encoding.toUint8Array(encoder), socket);
        });
      }
    );

    // forward changes to the Y.Doc to all connected clients
    this.ydoc.on('update', (update: Uint8Array, origin: unknown) => {
      const encoder = encoding.createEncoder();
      encoding.writeVarUint(encoder, MESSAGE_TYPE_SYNC);
      sync.writeUpdate(encoder, update);

      const message = encoding.toUint8Array(encoder);
      this.connections.forEach((clientIds, socket) => {
        if (socket !== origin) {
          sendMessage(message, socket);
        }
      });
    });
  }
}

function sendMessage(message: Uint8Array, socket: WebSocket) {
  if (socket.readyState !== WebSocket.CONNECTING && socket.readyState !== WebSocket.OPEN) {
    socket.close(1008);
  }

  try {
    socket.send(message);
  } catch (error) {
    console.error('Error sending message:', error);
    socket.close(1011, 'Error sending message');
  }
}
