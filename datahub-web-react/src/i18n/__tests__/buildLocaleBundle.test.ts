import fs from 'fs';
import os from 'os';
import path from 'path';
import { afterEach, describe, expect, it } from 'vitest';

import { NAMESPACES } from '@src/i18n/namespaces';

/* Vite plugin lives outside `src/` so Node can load it without path aliases. */
/* eslint-disable import-alias/import-alias, import/extensions */
import {
    buildLocaleBundle,
    listLocaleLanguages,
    listNamespaceGroups,
    namespaceGroup,
} from '../../../vite-plugins/i18nLocaleBundlesPlugin';

/* eslint-enable import-alias/import-alias, import/extensions */

describe('buildLocaleBundle', () => {
    const tmpDirs: string[] = [];

    afterEach(() => {
        tmpDirs.forEach((dir) => {
            fs.rmdirSync(dir, { recursive: true });
        });
        tmpDirs.length = 0;
    });

    function tmpDir(): string {
        const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'i18n-bundle-'));
        tmpDirs.push(dir);
        return dir;
    }

    it('merges each namespace json file into one object keyed by filename', () => {
        const dir = tmpDir();
        fs.writeFileSync(path.join(dir, 'alchemy.json'), JSON.stringify({ save: 'Save' }));
        fs.writeFileSync(path.join(dir, 'common.actions.json'), JSON.stringify({ delete: 'Delete' }));
        fs.writeFileSync(path.join(dir, 'notes.txt'), 'ignore me');

        expect(buildLocaleBundle(dir)).toEqual({
            alchemy: { save: 'Save' },
            'common.actions': { delete: 'Delete' },
        });
    });

    it('builds only the namespaces assigned to a requested group', () => {
        const dir = tmpDir();
        fs.writeFileSync(path.join(dir, 'common.actions.json'), JSON.stringify({ save: 'Save' }));
        fs.writeFileSync(path.join(dir, 'ingestion.json'), JSON.stringify({ run: 'Run' }));

        expect(buildLocaleBundle(dir, 'ingestion')).toEqual({
            ingestion: { run: 'Run' },
        });
    });

    it('assigns heavy feature namespaces outside the core group', () => {
        expect(namespaceGroup('common.actions')).toBe('core');
        expect(namespaceGroup('ingestion.sourceBuilder')).toBe('ingestion');
        expect(namespaceGroup('entity.profile.validations')).toBe('quality');
        expect(namespaceGroup('governance.domain')).toBe('governance');
        expect(namespaceGroup('saas.settings.ai')).toBe('settings');
        expect(namespaceGroup('saas.context.evals')).toBe('context');
        expect(namespaceGroup('saas.entity.profile.validations')).toBe('quality');
    });

    it('lists language directories and ignores files', () => {
        const locales = tmpDir();
        fs.mkdirSync(path.join(locales, 'de'));
        fs.mkdirSync(path.join(locales, 'en'));
        fs.writeFileSync(path.join(locales, 'README.md'), '');

        expect(listLocaleLanguages(locales)).toEqual(['de', 'en']);
    });

    it('includes every registered namespace from the English source files', () => {
        const enDir = path.resolve(__dirname, '../locales/en');
        const bundle = buildLocaleBundle(enDir);
        NAMESPACES.forEach((ns) => {
            expect(bundle).toHaveProperty(ns);
        });
        expect(Object.keys(listNamespaceGroups(enDir))).toEqual(expect.arrayContaining([...NAMESPACES]));
    });
});
