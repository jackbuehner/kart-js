import type * as decoding from 'lib0/decoding';
import * as encoding from 'lib0/encoding';
import { WebsocketProvider } from 'y-websocket';

export class KartProvider extends WebsocketProvider {
  constructor(...args: WebsockerProviderConstructorParameters) {
    super(...args);

    this.messageHandlers = new Proxy(this.messageHandlers, {
      get(target, prop, receiver) {
        if (prop === MESSAGE_TYPE_SELECT_RESPONSE.toString()) {
          return handleSelectResponseMessageType;
        }
        return Reflect.get(target, prop, receiver);
      },
    });
  }

  override emit(...args: KartProviderEvents) {
    super.emit(...(args as Parameters<WebsocketProvider['emit']>));
  }

  /**
   * Requests features from the server for a given dataset. Specify
   * the encoded IDs of the features that the server should send back
   * over the WebSocket connection. The server will respond with a message
   * containing the requested features, which can be listened for with the
   * 'select-response' event.
   */
  requestFeatures(datasetName: string, encodedIds: string[]) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      throw new Error('WebSocket is not open');
    }

    const message = `${datasetName}‾‾${encodedIds.join(',')}`;

    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, MESSAGE_TYPE_SELECT);
    encoding.writeVarString(encoder, message);
    this.ws.send(encoding.toUint8Array(encoder));
  }
}

type WebsockerProviderConstructorParameters = ConstructorParameters<typeof WebsocketProvider>;

type WebsocketProviderEvents = Parameters<WebsocketProvider['emit']>;
type KartProviderEvents = WebsocketProviderEvents | ['select-response', GeoJSON.Feature[]];

const MESSAGE_TYPE_SELECT = 102;
const MESSAGE_TYPE_SELECT_RESPONSE = 103;

function handleSelectResponseMessageType(
  encoder: encoding.Encoder,
  decoder: decoding.Decoder,
  provider: KartProvider,
  emitSynced: boolean,
  messageType: number
) {
  if (messageType === MESSAGE_TYPE_SELECT_RESPONSE) {
    const buffer = new Uint8Array(decoder.arr.slice(decoder.pos, decoder.arr.length));
    const jsonString = new TextDecoder().decode(buffer);
    const geojson = JSON.parse(jsonString);
    provider.emit('select-response', geojson);
    return true;
  }
}
