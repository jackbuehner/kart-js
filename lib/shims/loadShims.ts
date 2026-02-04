export async function loadShims() {
  await Promise.all([
    import('es-arraybuffer-base64/shim').then(({ default: shimArrayBufferBase64 }) => shimArrayBufferBase64()),
  ]);
}
