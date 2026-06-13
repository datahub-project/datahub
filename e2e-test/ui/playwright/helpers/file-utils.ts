import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { generateRandomString } from '../utils/random';

/**
 * Generate a unique file path with timestamp and random suffix.
 * Prevents file conflicts when multiple tests run in parallel.
 */
export function getFilePath(fileName: string, dirPath: string = os.tmpdir()): string {
  const uniqueSuffix = generateRandomString(8);
  return path.join(dirPath, `test-${Date.now()}-${uniqueSuffix}-${fileName}`);
}

/**
 * Write file content to a file and return the file path.
 */
export function writeFile(fileContent: string, fileName: string, dirPath: string = os.tmpdir()): string {
  const filePath = getFilePath(fileName, dirPath);
  fs.writeFileSync(filePath, fileContent);
  return filePath;
}

/**
 * Delete a file if it exists.
 */
export function deleteFileIfExists(filePath: string): void {
  if (fs.existsSync(filePath)) {
    fs.unlinkSync(filePath);
  }
}
