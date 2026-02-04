export class FileNotFoundError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'FileNotFoundError';
  }
}

export class FileReadError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'FileReadError';
  }
}

export class InvalidFileContentsError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidFileContentsError';
  }
}
