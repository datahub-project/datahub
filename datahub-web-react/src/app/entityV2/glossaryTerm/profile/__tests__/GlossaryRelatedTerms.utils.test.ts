import {
    glossaryRelatedTermsArePending,
    glossaryRelationshipSignature,
    pickGlossaryRelatedRelationships,
    relationshipEntries,
} from '@app/entityV2/glossaryTerm/profile/GlossaryRelatedTerms.utils';

describe('glossary related terms selection', () => {
    it('reads the relationship group for the selected menu key', () => {
        const source = {
            hasRelatedTerms: { relationships: [{ entity: { urn: 'urn:li:glossaryTerm:child' } }] },
            isRelatedTerms: { relationships: [] },
        };

        expect(relationshipEntries(source, 'hasRelatedTerms')).toEqual([
            { entity: { urn: 'urn:li:glossaryTerm:child' } },
        ]);
        expect(relationshipEntries(source, 'isRelatedTerms')).toEqual([]);
        expect(relationshipEntries(undefined, 'hasRelatedTerms')).toEqual([]);
    });

    it('uses a settled fetch, and the profile list while that fetch is in flight', () => {
        const kept = { entity: { urn: 'urn:li:glossaryTerm:kept' } };
        const removed = { entity: { urn: 'urn:li:glossaryTerm:removed' } };
        const profile = [kept];
        const fetched = [kept, removed];

        expect(pickGlossaryRelatedRelationships([], fetched, true)).toBe(fetched);
        expect(pickGlossaryRelatedRelationships(profile, [], true)).toBe(profile);
        expect(pickGlossaryRelatedRelationships(profile, fetched, true)).toBe(fetched);
        expect(pickGlossaryRelatedRelationships(profile, fetched, false)).toBe(profile);
    });

    it('changes the signature when a relationship is removed', () => {
        const keys = ['hasRelatedTerms'];
        const before = {
            hasRelatedTerms: {
                relationships: [
                    { entity: { urn: 'urn:li:glossaryTerm:kept' } },
                    { entity: { urn: 'urn:li:glossaryTerm:removed' } },
                ],
            },
        };
        const after = {
            hasRelatedTerms: { relationships: [{ entity: { urn: 'urn:li:glossaryTerm:kept' } }] },
        };

        expect(glossaryRelationshipSignature(before, keys)).not.toBe(glossaryRelationshipSignature(after, keys));
        expect(glossaryRelationshipSignature(before, keys)).toBe(glossaryRelationshipSignature(before, keys));
    });

    it('stays pending until the current term has relationships or both reads have settled empty', () => {
        expect(
            glossaryRelatedTermsArePending({
                urn: 'urn:li:glossaryTerm:next',
                relationshipCount: 0,
                profileReady: false,
                fetchedSettled: false,
            }),
        ).toBe(true);

        expect(
            glossaryRelatedTermsArePending({
                urn: 'urn:li:glossaryTerm:next',
                relationshipCount: 1,
                profileReady: false,
                fetchedSettled: false,
            }),
        ).toBe(false);

        expect(
            glossaryRelatedTermsArePending({
                urn: 'urn:li:glossaryTerm:next',
                relationshipCount: 0,
                profileReady: true,
                fetchedSettled: true,
            }),
        ).toBe(false);
    });
});
