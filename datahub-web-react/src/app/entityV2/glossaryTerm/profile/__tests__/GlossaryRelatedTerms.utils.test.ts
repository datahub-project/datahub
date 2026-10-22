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

    it('keeps the edited profile list until the fetched urns match it', () => {
        const kept = { entity: { urn: 'urn:li:glossaryTerm:kept' } };
        const removed = { entity: { urn: 'urn:li:glossaryTerm:removed' } };
        const profile = [kept];
        const staleFetched = [kept, removed];
        const settled = { fetchedSettled: true, holdForProfile: false, fetchFailed: false };

        expect(pickGlossaryRelatedRelationships([], staleFetched, settled)).toBe(staleFetched);
        expect(pickGlossaryRelatedRelationships(profile, staleFetched, { ...settled, holdForProfile: true })).toBe(
            profile,
        );
        expect(pickGlossaryRelatedRelationships(profile, staleFetched, { ...settled, fetchedSettled: false })).toBe(
            profile,
        );
        expect(pickGlossaryRelatedRelationships(profile, [kept], settled)).toEqual([kept]);
        expect(pickGlossaryRelatedRelationships([kept, removed], [], settled)).toEqual([]);
        expect(pickGlossaryRelatedRelationships(profile, staleFetched, { ...settled, fetchFailed: true })).toBe(
            profile,
        );
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

    it('shows an empty tab once this urn’s query has settled', () => {
        expect(
            glossaryRelatedTermsArePending({
                urn: 'urn:li:glossaryTerm:next',
                relationshipCount: 0,
                fetchedSettled: false,
            }),
        ).toBe(true);

        expect(
            glossaryRelatedTermsArePending({
                urn: 'urn:li:glossaryTerm:next',
                relationshipCount: 1,
                fetchedSettled: false,
            }),
        ).toBe(false);

        expect(
            glossaryRelatedTermsArePending({
                urn: 'urn:li:glossaryTerm:next',
                relationshipCount: 0,
                fetchedSettled: true,
            }),
        ).toBe(false);
    });
});
