import { test as base } from '@playwright/test';
import * as path from 'path';
import { FileLogger, type StructuredLogger } from '../utils/logger';




type MockingFixture = {
    mocker: ApiMocker
};
export type LoggingOptions = {

};


export const mockingFixture = base.extend<MockingFixture>({
    mocker: async ({ }, use, testInfo) => {
        mocker = PageApiMocker(...);
        use(mocker)
    },
});