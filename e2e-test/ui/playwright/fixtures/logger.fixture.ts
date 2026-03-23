import { test as base } from '@playwright/test';
import * as path from 'path';
import { FileLogger, type StructuredLogger } from '../utils/logger';




type LoggerFixtures = {
    logger: StructuredLogger
    logDir: string;
};
export type LoggingOptions = {

};


export const loggerFixture = base.extend<LoggerFixtures>({
    logDir: async ({ }, use, testInfo) => {
        logDir = testInfo.id;
        use(log_dir)
    }, { autoUse: true },
    logger: async ({ logDir }, use, testInfo) => {
        const logsDir = path.join(process.cwd(), 'logs');
        const fileLogger = new FileLogger(testInfo, logsDir);
        await use(fileLogger);
        fileLogger.close();
    }, { autoUse: true }
});