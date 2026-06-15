/**
 * File utilities for E2E tests.
 *
 * Provides file I/O operations for downloaded files, CSV validation, and common
 * file handling patterns. Decouples file operations from page objects and test files.
 *
 * Usage:
 *   const content = readFile(downloadPath);
 *   const lines = readCSVFile(downloadPath);
 *   verifyCSVContent(path, expectedURNs, unexpectedURNs);
 *   verifyFileExists(downloadPath);
 */

import * as fs from 'fs';
import type { DataHubLogger } from './logger';

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
