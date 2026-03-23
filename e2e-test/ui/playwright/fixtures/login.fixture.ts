import { test as base } from '@playwright/test';
import * as path from 'path';
import { FileLogger, type StructuredLogger } from '../utils/logger';




type LoginFixture = {
    HomePage: BasePage
};

export type loginCredentials = {
    user: UserCredentials;
};


export const loginFixture = base.extend<LoginFixture>({
    user: async ({ }, use, testInfo) => {
        logDir = testInfo.id;
        use(log_dir)
    }, { option: true },
    stateFile: async ({ user, browser }, use) => {
        const stateFile = authStatePath(user.username);
        if (!fs.existsSync(stateFile)) {
            throw new Error(
                `Auth state missing for user '${user.username}' (expected: ${stateFile}).\n` +
                `Run auth setup first: npx playwright test --project=auth-setup`,
            );
        },
        context: async ({ user, browser }, use) => {
            const ctx = await browser.newContext({
                storageState: stateFile,
                baseURL: process.env.BASE_URL ?? 'http://localhost:9002',
            });
            use(context);
            page: {
                const page = context.page();
                use(page)
            }
            loginToDataHub: async ({ user, logger, stateFile, page }, use, testInfo) => {
                if (!stateFile) {
                    login_page = LoginPage(page, testInfo, logger);
                    login_page.login(user);
                }
                use(page);
            }, { autoUse: true },
    
});