import { existsSync, readdirSync, readFileSync, realpathSync, statSync } from '@zenfs/core';
import * as path from '@zenfs/core/path';
import { FileNotFoundError, FileReadError } from './errors.ts';

export class Path {
  private fullPath: string;

  constructor(fullPath: string) {
    this.fullPath = fullPath.replaceAll('\\', '/');
  }

  /**
   * Joins the given segments into a single path and returns a new Path instance.
   */
  static join(...segments: string[]) {
    const joinedPath = segments.join('/').replaceAll('//', '/');
    return new Path(joinedPath);
  }

  /**
   * Joins the given segments to the current path and returns a new Path instance.
   */
  join(...segments: string[]) {
    const joinedPath = [this.fullPath, ...segments].join('/').replaceAll('//', '/');
    return new Path(joinedPath);
  }

  get absolute() {
    try {
      return realpathSync(this.fullPath).replaceAll('\\', '/');
    } catch (error) {
      const exposedError = new FileNotFoundError(`Invalid path: ${this.fullPath}`);
      exposedError.cause = error;
      throw exposedError;
    }
  }

  /**
   * Extracts the file name without the extension from the full path.
   */
  get basename(): string {
    const dirParts = this.fullPath.split('/');
    const fileName = dirParts[dirParts.length - 1];
    const dotIndex = fileName.lastIndexOf('.');
    return dotIndex === -1 ? fileName : fileName.substring(0, dotIndex);
  }

  get name(): string {
    const dirParts = this.fullPath.split('/');
    return dirParts[dirParts.length - 1];
  }

  get parentPath(): Path | null {
    const dirParts = this.fullPath.split('/');
    const parentParts = dirParts.slice(0, dirParts.length - 1);
    if (parentParts.length === 0) {
      return null;
    }
    return new Path(parentParts.join('/'));
  }

  /**
   * Extracts the file extension from the full path.
   */
  get extension(): string {
    const dirParts = this.fullPath.split('/');
    const fileName = dirParts[dirParts.length - 1];
    const dotIndex = fileName.lastIndexOf('.');
    return dotIndex === -1 ? '' : fileName.substring(dotIndex + 1);
  }

  get exists() {
    try {
      return existsSync(this.fullPath);
    } catch {
      return false;
    }
  }

  get isFile() {
    return statSync(this.fullPath).isFile();
  }

  get isDirectory() {
    return statSync(this.fullPath).isDirectory();
  }

  /**
   * Reads the file and returns its contents as a string.
   * @throws {FileReadError} If the file cannot be read.
   */
  readFileSync(options: { encoding: import('fs').EncodingOption; flag?: string }): string;
  /**
   * Reads the file and returns its contents as a Uint8Array.
   * @throws {FileReadError} If the file cannot be read.
   * @throws {FileNotFoundError} If the file does not exist.
   */
  readFileSync(options?: { flag?: string }): Uint8Array;
  readFileSync(options?: { encoding?: import('fs').EncodingOption; flag?: string }): string | Uint8Array {
    if (!this.exists) {
      throw new FileNotFoundError(`File does not exist at path: ${this.fullPath}`);
    }

    if (!this.isFile) {
      throw new FileReadError(`Path is not a file: ${this.fullPath}`);
    }

    try {
      if (options?.encoding) {
        return readFileSync(this.fullPath, options) as unknown as string;
      }
      const buffer = readFileSync(this.fullPath, options);
      return new Uint8Array(buffer);
    } catch (error) {
      const exposedError = new FileReadError(`Failed to read file at path: ${this.fullPath}`);
      exposedError.cause = error;
      throw exposedError;
    }
  }

  /**
   * Reads the directory and returns an array of Path instances for each entry.
   * @throws {FileNotFoundError} If the directory does not exist.
   * @throws {FileReadError} If the directory cannot be read.
   */
  readDirectorySync(options?: { recursive?: boolean; encoding?: BufferEncoding }): Path[] {
    if (!this.exists) {
      throw new FileNotFoundError(`Directory does not exist at path: ${this.fullPath}`);
    }

    if (!this.isDirectory) {
      throw new FileReadError(`Path is not a directory: ${this.fullPath}`);
    }

    try {
      return readdirSync(this.fullPath, { recursive: options?.recursive, encoding: 'utf-8' }).map((name) =>
        this.join(name)
      );
    } catch (error) {
      const exposedError = new FileReadError(`Failed to read directory at path: ${this.fullPath}`);
      exposedError.cause = error;
      throw exposedError;
    }
  }

  toString() {
    return this.fullPath;
  }

  toJSON() {
    return this.fullPath;
  }

  /**
   * Gets a relative path string from this path to the given path.
   */
  relativeTo(to: Path): string {
    return path.relative(this.absolute, to.absolute).replaceAll('\\', '/');
  }
}
