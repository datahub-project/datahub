/**
 * Structured logging utility — Winston-based logger factory.
 *
 * Provides a DataHubLogger that writes structured JSON logs with context
 * metadata (suite, test, worker, retry). Transports are configured per
 * environment:
 *
 *   Local (non-CI): console (colorized) only
 *   CI:             console + test.log + error.log in the test-specific logDir
 *
 * Usage:
 *   - Construct via createLogger(logDir, testMeta) from the logger fixture.
 *   - Never import winston directly in tests or page objects; use DataHubLogger.
 *   - Import the DataHubLogger type here when you need it in page objects.
 */

import * as fs from 'fs';
import * as path from 'path';
import winston from 'winston';

// ── Public interface ──────────────────────────────────────────────────────────

export interface DataHubLogger {
  info(message: string, data?: Record<string, unknown>): void;
  warn(message: string, data?: Record<string, unknown>): void;
  error(message: string, data?: Record<string, unknown>): void;
  /** Log a named test step — use instead of test.step when a structured
   *  record in the log file is more useful than an HTML report step. */
  step(name: string, data?: Record<string, unknown>): void;
}

export interface TestMeta {
  suite: string;
  test: string;
  worker: number;
  retry: number;
}

// ── Implementation ────────────────────────────────────────────────────────────

const consoleFormat = winston.format.combine(
  winston.format.colorize(),
  winston.format.printf(({ level, message, ...meta }) => {
    const extras = Object.keys(meta).length > 0 ? ` ${JSON.stringify(meta)}` : '';
    return `${level}: ${message as string}${extras}`;
  }),
);

const fileFormat = winston.format.combine(winston.format.timestamp(), winston.format.json());

/**
 * Lightweight logger for scripts that run outside the Playwright fixture scope
 * (setup files, standalone utilities). Always writes to console; never to file.
 */
export function createScriptLogger(script: string): DataHubLogger {
  const winstonLogger = winston.createLogger({
    defaultMeta: { script },
    transports: [new winston.transports.Console({ format: consoleFormat })],
  });
  return {
    info: (message, data) => winstonLogger.info(message, data),
    warn: (message, data) => winstonLogger.warn(message, data),
    error: (message, data) => winstonLogger.error(message, data),
    step: (name, data) => winstonLogger.info(name, { ...data, isStep: true }),
  };
}

export function createLogger(logDir: string, testMeta: TestMeta): DataHubLogger {
  const transports: winston.transport[] = [];

  if (!process.env.CI || !logDir) {
    transports.push(new winston.transports.Console({ format: consoleFormat }));
  }

  // In CI write structured JSON logs to files so they can be archived.
  // Append the retry index to the filename so retried attempts don't overwrite the first run.
  // logDir may be empty for infrastructure fixtures (e.g. seeding); skip file transport in that case.
  if (process.env.CI && logDir) {
    fs.mkdirSync(logDir, { recursive: true });
    const retrySuffix = testMeta.retry > 0 ? `-retry${testMeta.retry}` : '';
    transports.push(
      new winston.transports.File({
        filename: path.join(logDir, `test${retrySuffix}.log`),
        format: fileFormat,
      }),
      new winston.transports.File({
        filename: path.join(logDir, `error${retrySuffix}.log`),
        level: 'error',
        format: fileFormat,
      }),
    );
  }

  const winstonLogger = winston.createLogger({
    defaultMeta: testMeta,
    transports,
    // Silence the "no transports" warning in environments without files.
    silent: false,
  });

  return {
    info: (message, data) => winstonLogger.info(message, data),
    warn: (message, data) => winstonLogger.warn(message, data),
    error: (message, data) => winstonLogger.error(message, data),
    step: (name, data) => winstonLogger.info(name, { ...data, isStep: true }),
  };
}
