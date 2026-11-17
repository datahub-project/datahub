import { glossaryTermUrlPreservingTab } from '@app/entityV2/glossaryTerm/glossaryTermUrl';

const BASE = '/glossaryTerm/urn:li:glossaryTerm:next';

describe('glossaryTermUrlPreservingTab', () => {
    it('keeps the Related Terms tab when opening another term from a glossary term page', () => {
        expect(
            glossaryTermUrlPreservingTab(
                BASE,
                '/glossaryTerm/urn:li:glossaryTerm:current/Related Terms',
                'Related Terms?is_lineage_mode=false',
            ),
        ).toBe(`${BASE}/Related Terms?is_lineage_mode=false`);
    });

    it('keeps lineage mode and drops search state from the current tab', () => {
        expect(
            glossaryTermUrlPreservingTab(
                BASE,
                '/glossaryTerm/urn:li:glossaryTerm:current/Related Assets',
                'Related Assets?is_lineage_mode=false&page=2&query=pii',
            ),
        ).toBe(`${BASE}/Related Assets?is_lineage_mode=false`);
    });

    it('drops the tab while editing documentation and when the current page is not a glossary term', () => {
        expect(
            glossaryTermUrlPreservingTab(
                BASE,
                '/glossaryTerm/urn:li:glossaryTerm:current/Documentation',
                'Documentation?editing=true',
            ),
        ).toBe(BASE);

        expect(
            glossaryTermUrlPreservingTab(BASE, '/dataset/urn:li:dataset:one/Schema', 'Schema?is_lineage_mode=false'),
        ).toBe(BASE);
    });
});
