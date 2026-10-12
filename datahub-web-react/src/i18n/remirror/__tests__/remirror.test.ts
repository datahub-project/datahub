import { messages as remirrorEnMessages } from '@remirror/i18n/en';

import { LOCALE_MAP } from '@app/i18n/constants';
import { REMIRROR_LOCALE_LOADERS } from '@src/i18n/remirror';

// Remirror renders the raw message id for any key missing from the active locale's bundle,
// so each supplementary locale must cover every message id Remirror ships for English.
// This guards against drift when `@remirror/i18n` adds keys on upgrade.
describe('Remirror locale bundles', () => {
    const enKeys = Object.keys(remirrorEnMessages);

    it.each(Object.entries(REMIRROR_LOCALE_LOADERS))(
        '"%s" covers every Remirror message id',
        async (_locale, loader) => {
            const { default: messages } = await loader();
            const missing = enKeys.filter((key) => !(key in messages));
            expect(missing).toEqual([]);
        },
    );
});

// LOCALE_MAP is the source of truth for supported languages. Every non-English language must
// have a Remirror bundle wired up here, or the editor's toolbar labels silently render in
// English for that language. English is provided by `@remirror/i18n`, so it has no bundle.
describe('Remirror locale coverage', () => {
    const nonEnglishLanguages = Object.keys(LOCALE_MAP).filter((lang) => lang !== 'en');

    it.each(nonEnglishLanguages)('has a locale bundle registered for %s', (lang) => {
        expect(Object.keys(REMIRROR_LOCALE_LOADERS)).toContain(lang);
    });
});
