import { describe, expect, it } from 'vitest';

import { mergeFilterChanges } from '@app/searchV2/filters/mergeFilterChanges';

const platform = { field: 'platform', values: ['urn:li:dataPlatform:hive'] };
const domain = { field: 'domains', values: ['urn:li:domain:a'] };
const owner = { field: 'owners', values: ['urn:li:corpuser:a'] };

describe('mergeFilterChanges', () => {
    it('keeps changes from several menus built from the same base', () => {
        const base = [platform];
        const afterDomain = mergeFilterChanges(base, base, [platform, domain]);
        const afterOwner = mergeFilterChanges(base, afterDomain, [platform, owner]);

        expect(afterOwner).toEqual([platform, domain, owner]);
    });

    it('applies a removed field', () => {
        const base = [platform, domain];
        expect(mergeFilterChanges(base, base, [platform])).toEqual([platform]);
    });

    it('returns the current filters when nothing changed', () => {
        const base = [platform];
        const current = [platform, domain];
        expect(mergeFilterChanges(base, current, [platform])).toBe(current);
    });
});
