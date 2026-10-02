import { injectGlobalSearchVariables } from '@app/appConfig/injectedOperationVariables';

const flagsOff = { showSeparateSiblings: false, hideLineageInSearchCards: false };
const hideLineage = { showSeparateSiblings: false, hideLineageInSearchCards: true };

describe('injectGlobalSearchVariables', () => {
    it('leaves lineage on when neither the flag nor the caller skips it', () => {
        expect(injectGlobalSearchVariables({ input: { query: 'events' } }, flagsOff)).toEqual({
            input: { query: 'events' },
            skipSiblingsSearch: false,
            skipLineage: false,
        });
    });

    it('keeps an explicit skip so search results are not blocked on per-card lineage', () => {
        expect(injectGlobalSearchVariables({ skipLineage: true }, flagsOff).skipLineage).toBe(true);
    });

    it('still skips lineage when the hide-lineage flag is on', () => {
        expect(injectGlobalSearchVariables({}, hideLineage).skipLineage).toBe(true);
        expect(injectGlobalSearchVariables({ skipLineage: false }, hideLineage).skipLineage).toBe(true);
    });
});
