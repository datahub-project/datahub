import {
    glossaryRelatedTermsArePending,
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

    it('prefers the longer list so an empty profile payload does not hide fetched terms', () => {
        const profile = [{ entity: { urn: 'urn:li:glossaryTerm:from-profile' } }];
        const fetched = [
            { entity: { urn: 'urn:li:glossaryTerm:from-fetch-1' } },
            { entity: { urn: 'urn:li:glossaryTerm:from-fetch-2' } },
        ];

        expect(pickGlossaryRelatedRelationships([], fetched)).toBe(fetched);
        expect(pickGlossaryRelatedRelationships(profile, [])).toBe(profile);
        expect(pickGlossaryRelatedRelationships(profile, fetched)).toBe(fetched);
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
