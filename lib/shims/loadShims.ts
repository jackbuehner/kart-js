import { Temporal } from 'temporal-polyfill';

export async function loadShims() {
  await Promise.all([
    import('es-arraybuffer-base64/shim').then(({ default: shimArrayBufferBase64 }) => shimArrayBufferBase64()),
  ]);
}

// ensure that the temporal dates print nicely in the console
(async () => {
  if (process.env.TARGET === 'node') {
    const { inspect } = await import('node:util');
    type InspectOptions = Parameters<typeof inspect>[1];

    (Temporal.PlainDate.prototype as any)[inspect.custom] = function (
      this: Temporal.PlainDate,
      depth: number,
      opts: InspectOptions
    ) {
      return (opts as any).stylize(this.toString(), 'date');
    };
    (Temporal.Instant.prototype as any)[inspect.custom] = function (
      this: Temporal.Instant,
      depth: number,
      opts: InspectOptions
    ) {
      return (opts as any).stylize(this.toString(), 'date');
    };
    (Temporal.PlainTime.prototype as any)[inspect.custom] = function (
      this: Temporal.PlainTime,
      depth: number,
      opts: InspectOptions
    ) {
      return (opts as any).stylize(this.toString(), 'date');
    };
    (Temporal.PlainDateTime.prototype as any)[inspect.custom] = function (
      this: Temporal.PlainDateTime,
      depth: number,
      opts: InspectOptions
    ) {
      return (opts as any).stylize(this.toString(), 'date');
    };
  }
})();
