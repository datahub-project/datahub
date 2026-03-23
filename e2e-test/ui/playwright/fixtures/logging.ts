/**
 * Structured logging capability.
 *
 * Exposes a `StructuredLogger` that writes JSON-lines to `logs/test-run.log`.
 * Every entry includes timestamp, log level, test title, suite path, worker
 * index, and retry count — giving the Structured Log File report described in
 * the reporting requirements.
 *
 * The logger is provided as a Playwright fixture by base-test.ts.
 * Import `StructuredLogger` here when you need the type in page objects or
 * helpers; never construct `FileLogger` directly in test code.
 */

import * as fs from 'fs';
import * as path from 'path';
import type { TestInfo } from '@playwright/test';

// ── Public interface ──────────────────────────────────────────────────────────

export interface StructuredLogger {
  info(message: string, data?: Record<string, unknown>): void;
  warn(message: string, data?: Record<string, unknown>): void;
  error(message: string, data?: Record<string, unknown>): void;
  /** Log a named test step. Use in place of `test.step` when a structured
   *  record in the log file is more useful than the HTML report step. */
  step(name: string, data?: Record<string, unknown>): void;
}

// ── Implementation ────────────────────────────────────────────────────────────

interface LogEntry {
  timestamp: string;
  level: string;
  suite: string;
  test: string;
  worker: number;
  retry: number;
  message: string;
  [key: string]: unknown;
}

export class FileLogger implements StructuredLogger {
  private readonly stream: fs.WriteStream;
  private readonly meta: Pick<LogEntry, 'suite' | 'test' | 'worker' | 'retry'>;

  constructor(testInfo: TestInfo, logsDir: string) {
    fs.mkdirSync(logsDir, { recursive: true });
    this.stream = fs.createWriteStream(path.join(logsDir, 'test-run.log'), { flags: 'a' });
    this.meta = {
      suite: testInfo.titlePath.slice(0, -1).join(' > '),
      test: testInfo.title,
      worker: testInfo.workerIndex,
      retry: testInfo.retry,
    };
  }

  info(message: string, data?: Record<string, unknown>): void {
    this.write('INFO', message, data);
  }

  warn(message: string, data?: Record<string, unknown>): void {
    this.write('WARN', message, data);
  }

  error(message: string, data?: Record<string, unknown>): void {
    this.write('ERROR', message, data);
  }

  step(name: string, data?: Record<string, unknown>): void {
    this.write('STEP', name, data);
  }

  close(): void {
    this.stream.end();
  }

  private write(level: string, message: string, data?: Record<string, unknown>): void {
    const entry: LogEntry = {
      timestamp: new Date().toISOString(),
      level,
      ...this.meta,
      message,
      ...data,
    };
    this.stream.write(JSON.stringify(entry) + '\n');
  }
}
