import { DocumentTreeNode } from '@app/document/DocumentTreeContext';
import {
    DEFAULT_DOCUMENT_SIDEBAR_SORT,
    DOCUMENT_SIDEBAR_SORT,
    documentSidebarSortToCriterion,
} from '@app/document/utils/documentSidebarSort';
import { sortDocumentTreeNodes } from '@app/document/utils/sortDocumentTreeNodes';

import { SortOrder } from '@types';

describe('documentSidebarSort', () => {
    it('maps name and lastModified selections to search sortCriterion', () => {
        expect(documentSidebarSortToCriterion(DOCUMENT_SIDEBAR_SORT.NAME_ASC)).toEqual({
            field: '_entityName',
            sortOrder: SortOrder.Ascending,
        });
        expect(documentSidebarSortToCriterion(DOCUMENT_SIDEBAR_SORT.NAME_DESC)).toEqual({
            field: '_entityName',
            sortOrder: SortOrder.Descending,
        });
        expect(documentSidebarSortToCriterion(DOCUMENT_SIDEBAR_SORT.LAST_MODIFIED_DESC)).toEqual({
            field: 'lastModifiedAt',
            sortOrder: SortOrder.Descending,
        });
    });

    it('defaults to name A–Z', () => {
        expect(DEFAULT_DOCUMENT_SIDEBAR_SORT).toBe(DOCUMENT_SIDEBAR_SORT.NAME_ASC);
    });
});

describe('sortDocumentTreeNodes', () => {
    const nodes: DocumentTreeNode[] = [
        { urn: 'b', title: 'Bravo', parentUrn: null, hasChildren: false },
        { urn: 'a', title: 'Alpha', parentUrn: null, hasChildren: false },
        { urn: 'c', title: 'charlie', parentUrn: null, hasChildren: false },
    ];

    it('sorts by title A–Z and Z–A', () => {
        expect(sortDocumentTreeNodes(nodes, DOCUMENT_SIDEBAR_SORT.NAME_ASC).map((n) => n.urn)).toEqual(['a', 'b', 'c']);
        expect(sortDocumentTreeNodes(nodes, DOCUMENT_SIDEBAR_SORT.NAME_DESC).map((n) => n.urn)).toEqual([
            'c',
            'b',
            'a',
        ]);
    });

    it('sorts letters before numbers and ignores leading punctuation', () => {
        const funky: DocumentTreeNode[] = [
            { urn: 'seed', title: '[SEED] Expand', parentUrn: null, hasChildren: false },
            { urn: 'num', title: '50', parentUrn: null, hasChildren: false },
            { urn: 'and', title: 'And another', parentUrn: null, hasChildren: false },
            { urn: 'draft', title: 'Draft', parentUrn: null, hasChildren: false },
            { urn: 'doc', title: 'doc-expand_parent', parentUrn: null, hasChildren: false },
        ];
        expect(sortDocumentTreeNodes(funky, DOCUMENT_SIDEBAR_SORT.NAME_ASC).map((n) => n.urn)).toEqual([
            'and',
            'doc',
            'draft',
            'seed',
            'num',
        ]);
    });

    it('sorts by lastModified newest first', () => {
        const timed: DocumentTreeNode[] = [
            { urn: 'old', title: 'Old', parentUrn: null, hasChildren: false, lastModifiedAt: 100 },
            { urn: 'new', title: 'New', parentUrn: null, hasChildren: false, lastModifiedAt: 300 },
            { urn: 'mid', title: 'Mid', parentUrn: null, hasChildren: false, lastModifiedAt: 200 },
        ];
        expect(sortDocumentTreeNodes(timed, DOCUMENT_SIDEBAR_SORT.LAST_MODIFIED_DESC).map((n) => n.urn)).toEqual([
            'new',
            'mid',
            'old',
        ]);
    });
});
