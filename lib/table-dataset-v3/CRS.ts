import parseWkt from 'wkt-parser';
import { Enumerable } from '../utils/Enumerable.ts';
import { FileNotFoundError, InvalidFileContentsError } from '../utils/errors.ts';
import type { Path } from '../utils/Path.ts';

export class CRS {
  identifier: string;
  wkt: string;

  constructor(identifier: string, wkt: string) {
    this.identifier = identifier;
    this.wkt = wkt;
  }

  static fromWktFile(filePath: Path) {
    if (!filePath.exists || !filePath.isFile) {
      throw new FileNotFoundError(`Legend file does not exist at path: ${filePath}`);
    }

    if (filePath.extension !== 'wkt') {
      throw new FileNotFoundError(
        `CRS definition file must have a .wkt extension. Got "${filePath.extension}".`
      );
    }

    const fileContents = filePath.readFileSync({ encoding: 'utf-8' });

    try {
      parseWkt(fileContents); // throws an error if the WKT is invalid
    } catch (error) {
      throw new InvalidFileContentsError(
        `Invalid WKT contents: ${error instanceof Error ? error.message : String(error)}`
      );
    }

    return new CRS(filePath.basename, fileContents);
  }
}

export class CRSs extends Enumerable<CRS> {}
