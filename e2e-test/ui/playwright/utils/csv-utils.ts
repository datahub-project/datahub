/**
 * CSV file utilities for lineage download testing.
 *
 * Provides file I/O and validation operations for CSV files downloaded
 * from lineage degree filter tests. Decouples file operations from page
 * object models and test files to follow utility pattern.
 *
 * Usage:
 *   const path = await lineagePage.downloadCSV(context, filename);
 *   await verifyCSVContent(path, expectedURNs, unexpectedURNs);
 */

import * as fs from 'fs';
import type { DataHubLogger } from './logger';

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
  let csvContent: string;

  try {
    csvContent = fs.readFileSync(downloadPath, 'utf-8');
    logger?.info('csv-utils: read CSV file', { downloadPath });
  } catch (error) {
    const message = `Failed to read CSV file: ${downloadPath}`;
    logger?.error('csv-utils: file read error', { downloadPath, error });
    throw new Error(message);
  }

  shouldContain.forEach((urn) => {
    if (!csvContent.includes(urn)) {
      const message = `Expected URN not found in CSV: ${urn}`;
      logger?.error('csv-utils: missing URN', { urn, downloadPath });
      throw new Error(message);
    }
  });

  shouldNotContain.forEach((urn) => {
    if (csvContent.includes(urn)) {
      const message = `Unexpected URN found in CSV: ${urn}`;
      logger?.error('csv-utils: unexpected URN', { urn, downloadPath });
      throw new Error(message);
    }
  });

  logger?.info('csv-utils: CSV content verified', { downloadPath, expectedCount: shouldContain.length });
}

/**
 * Read CSV file and return content as string.
 * Useful for direct inspection or custom validation logic.
 */
export function readCSVFile(downloadPath: string, logger?: DataHubLogger): string {
  try {
    const content = fs.readFileSync(downloadPath, 'utf-8');
    logger?.info('csv-utils: read CSV file', { downloadPath });
    return content;
  } catch (error) {
    const message = `Failed to read CSV file: ${downloadPath}`;
    logger?.error('csv-utils: file read error', { downloadPath, error });
    throw new Error(message);
  }
}
