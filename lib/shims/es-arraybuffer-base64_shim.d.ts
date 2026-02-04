declare module 'es-arraybuffer-base64/shim' {
  export default function shimArrayBufferBase64(): void;
}

interface Uint8Array {
  /**
   * Encodes the Uint8Array into a hex string.
   */
  toHex(): string;

  /**
   * Encodes the Uint8Array into a base64 string.
   */
  toBase64(options?: { alphabet?: 'base64' | 'base64url'; omitPadding?: boolean }): string;

  /**
   * Decodes a hex string into this Uint8Array.
   */
  setFromHex(hexString: string): { read: number; written: number };

  /**
   * Decodes a base64 string into this Uint8Array.
   */
  setFromBase64(
    base64String: string,
    options?: { alphabet?: 'base64' | 'base64url' }
  ): { read: number; written: number };
}

interface Uint8ArrayConstructor {
  /**
   * Creates a Uint8Array from a hex string.
   */
  fromHex(hexString: string): Uint8Array;

  /**
   * Creates a Uint8Array from a base64 string.
   */
  fromBase64(base64String: string, options?: { alphabet?: 'base64' | 'base64url' }): Uint8Array;
}
