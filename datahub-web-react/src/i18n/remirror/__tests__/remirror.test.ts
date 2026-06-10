import { messages as remirrorEnMessages } from '@remirror/i18n/en';

import { REMIRROR_LOCALE_MESSAGES } from '@src/i18n/remirror';

// Remirror renders the raw message id for any key missing from the active locale's bundle,
// so each supplementary locale must cover every message id Remirror ships for English.
// This guards against drift when `@remirror/i18n` adds keys on upgrade.
describe('Remirror locale bundles', () => {
    const enKeys = Object.keys(remirrorEnMessages);

    it.each(Object.entries(REMIRROR_LOCALE_MESSAGES))('"%s" covers every Remirror message id', (_locale, messages) => {
        const missing = enKeys.filter((key) => !(key in messages));
        expect(missing).toEqual([]);
    });
});
