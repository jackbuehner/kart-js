/**
 * Creates a debounced version of an async function that prevents
 * concurrent executions.
 *
 * @param fn - The asynchronous function to debounce.
 * @param ms - Delay in milliseconds before invoking the function.
 * @returns A wrapped function.
 */
export function debounce<T extends (...args: any[]) => Promise<any>>(
  fn: T,
  ms: number
): (...args: Parameters<T>) => void {
  let timeoutId: ReturnType<typeof setTimeout> | undefined;
  let isExecuting = false;

  const run = (...args: Parameters<T>) => {
    // clear existing timer to reset the delay
    if (timeoutId !== undefined) {
      clearTimeout(timeoutId);
    }

    // schedule execution after the specified delay
    timeoutId = setTimeout(async () => {
      // prevent execution if a previous run is still pending
      if (isExecuting) {
        run(...args); // try again
        return;
      }

      isExecuting = true;
      try {
        await fn(...args);
      } catch (error) {
        console.error('Debounced function execution failed:', error);
      } finally {
        isExecuting = false;
      }
    }, ms);
  };

  return run;
}
