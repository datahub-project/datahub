/**
 * File utilities for E2E tests.
 *
 * Provides file I/O operations including:
 * - Reading files and CSV content
 * - Writing and deleting files
 * - File verification and inspection
 * - Path generation for test files with unique naming
 *
 * Usage:
 *   const content = readFile(downloadPath);
 *   const lines = readCSVFile(downloadPath);
 *   verifyCSVContent(path, expectedURNs, unexpectedURNs);
 *   verifyFileExists(downloadPath);
 *   const filePath = getFilePath('test.csv');
 *   writeFile(content, 'test.csv');
 *   deleteFileIfExists(filePath);
 */

import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import type { DataHubLogger } from './logger';
import { generateRandomString } from './random';

/**
 * Read file and return content as string.
 * Throws error if file cannot be read.
 */
export function readFile(filePath: string, logger?: DataHubLogger): string {
  try {
    const content = fs.readFileSync(filePath, 'utf-8');
    logger?.info('file-utils: read file', { filePath });
    return content;
  } catch (error) {
    const message = `Failed to read file: ${filePath}`;
    logger?.error('file-utils: file read error', { filePath, error });
    throw new Error(message);
  }
}

/**
 * Read CSV file and return content as string.
 * Useful for direct inspection or custom validation logic.
 */
export function readCSVFile(downloadPath: string, logger?: DataHubLogger): string {
  return readFile(downloadPath, logger);
}

/**
 * Verify downloaded CSV file contains expected URNs and excludes unwanted URNs.
 * Throws error with specific URN if verification fails.
 */
export function verifyCSVContent(
  downloadPath: string,
  shouldContain: string[],
  shouldNotContain: string[],
  logger?: DataHubLogger,
): void {
  const csvContent = readFile(downloadPath, logger);

  shouldContain.forEach((urn) => {
    if (!csvContent.includes(urn)) {
      const message = `Expected URN not found in CSV: ${urn}`;
      logger?.error('file-utils: missing URN', { urn, downloadPath });
      throw new Error(message);
    }
  });

  shouldNotContain.forEach((urn) => {
    if (csvContent.includes(urn)) {
      const message = `Unexpected URN found in CSV: ${urn}`;
      logger?.error('file-utils: unexpected URN', { urn, downloadPath });
      throw new Error(message);
    }
  });

  logger?.info('file-utils: CSV content verified', { downloadPath, expectedCount: shouldContain.length });
}

/**
 * Verify file exists at the given path.
 * Throws error if file does not exist.
 */
export function verifyFileExists(filePath: string, logger?: DataHubLogger): void {
  try {
    fs.accessSync(filePath);
    logger?.info('file-utils: file exists', { filePath });
  } catch (error) {
    const message = `File not found: ${filePath}`;
    logger?.error('file-utils: file not found', { filePath, error });
    throw new Error(message);
  }
}

/**
 * Get file size in bytes.
 */
export function getFileSize(filePath: string, logger?: DataHubLogger): number {
  try {
    const stats = fs.statSync(filePath);
    logger?.info('file-utils: got file size', { filePath, size: stats.size });
    return stats.size;
  } catch (error) {
    const message = `Failed to get file size: ${filePath}`;
    logger?.error('file-utils: stat error', { filePath, error });
    throw new Error(message);
  }
}

/**
 * Delete file at the given path.
 */
export function deleteFile(filePath: string, logger?: DataHubLogger): void {
  try {
    fs.unlinkSync(filePath);
    logger?.info('file-utils: file deleted', { filePath });
  } catch (error) {
    const message = `Failed to delete file: ${filePath}`;
    logger?.error('file-utils: delete error', { filePath, error });
    throw new Error(message);
  }
}

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
