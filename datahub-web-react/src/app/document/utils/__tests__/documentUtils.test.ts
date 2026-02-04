import { describe, expect, it } from 'vitest';

import {
    documentToTreeNode,
    extractUrnsFromMarkdown,
    hasBalancedParens,
    isAllowedRelatedAssetUrn,
    sortDocumentsByCreationTime,
} from '@app/document/utils/documentUtils';

import { Document } from '@types';

// Helper to create minimal valid Document for testing
// Uses 'as any' to allow partial documents in tests (consistent with other test files)
const createTestDocument = (overrides: Partial<Document> = {}): Document => {
    const base = {
        urn: 'urn:li:document:test',
        info: {
            title: 'Test Document',
            created: { time: 1000 },
            contents: { text: '' },
            lastModified: { time: 1000 },
        },
    };
    return {
        ...base,
        ...overrides,
        info: {
            ...base.info,
            ...overrides.info,
        },
    } as Document;
};

describe('documentUtils', () => {
    describe('documentToTreeNode', () => {
        it('should convert a document with all fields to a tree node', () => {
            const doc = createTestDocument({
                urn: 'urn:li:document:123',
                info: {
                    title: 'Test Document',
                    parentDocument: {
                        document: {
                            urn: 'urn:li:document:parent',
                        },
                    },
                } as any,
            });

            const result = documentToTreeNode(doc, true);

            expect(result).toEqual({
                urn: 'urn:li:document:123',
                title: 'Test Document',
                parentUrn: 'urn:li:document:parent',
                hasChildren: true,
                children: undefined,
            });
        });

        it('should use "Untitled" as default title when title is missing', () => {
            const doc = createTestDocument({
                urn: 'urn:li:document:123',
                info: {
                    title: null,
                } as any,
            });

            const result = documentToTreeNode(doc, false);

            expect(result.title).toBe('Untitled');
        });

        it('should use "Untitled" as default title when info is missing', () => {
            const doc = createTestDocument({
                urn: 'urn:li:document:123',
            });
            // Override to null for this test case
            (doc as any).info = null;

            const result = documentToTreeNode(doc, false);

            expect(result.title).toBe('Untitled');
        });

        it('should set parentUrn to null when parentDocument is missing', () => {
            const doc = createTestDocument({
                urn: 'urn:li:document:123',
                info: {
                    title: 'Root Document',
                    parentDocument: null,
                } as any,
            });

            const result = documentToTreeNode(doc, false);

            expect(result.parentUrn).toBe(null);
        });

        it('should set parentUrn to null when parentDocument.document is missing', () => {
            const doc = createTestDocument({
                urn: 'urn:li:document:123',
                info: {
                    title: 'Test Document',
                    parentDocument: {
                        document: null,
                    },
                } as any,
            });

            const result = documentToTreeNode(doc, false);

            expect(result.parentUrn).toBe(null);
        });

        it('should correctly set hasChildren flag', () => {
            const doc = createTestDocument({
                urn: 'urn:li:document:123',
                info: {
                    title: 'Test Document',
                } as any,
            });

            const resultWithChildren = documentToTreeNode(doc, true);
            expect(resultWithChildren.hasChildren).toBe(true);

            const resultWithoutChildren = documentToTreeNode(doc, false);
            expect(resultWithoutChildren.hasChildren).toBe(false);
        });

        it('should always set children to undefined', () => {
            const doc = createTestDocument({
                urn: 'urn:li:document:123',
                info: {
                    title: 'Test Document',
                } as any,
            });

            const result = documentToTreeNode(doc, true);

            expect(result.children).toBeUndefined();
        });
    });

    describe('sortDocumentsByCreationTime', () => {
        it('should sort documents by creation time in descending order (newest first)', () => {
            const documents: Document[] = [
                createTestDocument({
                    urn: 'urn:li:document:1',
                    info: {
                        title: 'Oldest',
                        created: { time: 1000 },
                    } as any,
                }),
                createTestDocument({
                    urn: 'urn:li:document:2',
                    info: {
                        title: 'Newest',
                        created: { time: 3000 },
                    } as any,
                }),
                createTestDocument({
                    urn: 'urn:li:document:3',
                    info: {
                        title: 'Middle',
                        created: { time: 2000 },
                    } as any,
                }),
            ];

            const result = sortDocumentsByCreationTime(documents);

            expect(result[0].urn).toBe('urn:li:document:2'); // Newest first
            expect(result[1].urn).toBe('urn:li:document:3');
            expect(result[2].urn).toBe('urn:li:document:1'); // Oldest last
        });

        it('should handle documents with missing creation time (treat as 0)', () => {
            const documents: Document[] = [
                createTestDocument({
                    urn: 'urn:li:document:1',
                    info: {
                        title: 'Has time',
                        created: { time: 2000 },
                    } as any,
                }),
                createTestDocument({
                    urn: 'urn:li:document:2',
                    info: {
                        title: 'No time',
                        created: null as any,
                    } as any,
                }),
                createTestDocument({
                    urn: 'urn:li:document:3',
                    info: {
                        title: 'No created field',
                        created: undefined as any,
                    } as any,
                }),
            ];

            const result = sortDocumentsByCreationTime(documents);

            // Documents with time should come first
            expect(result[0].urn).toBe('urn:li:document:1');
            // Documents without time should be sorted together (both treated as 0)
            expect(result.slice(1).map((d) => d.urn)).toContain('urn:li:document:2');
            expect(result.slice(1).map((d) => d.urn)).toContain('urn:li:document:3');
        });

        it('should not mutate the original array', () => {
            const documents: Document[] = [
                createTestDocument({
                    urn: 'urn:li:document:1',
                    info: {
                        title: 'Doc 1',
                        created: { time: 1000 },
                    } as any,
                }),
                createTestDocument({
                    urn: 'urn:li:document:2',
                    info: {
                        title: 'Doc 2',
                        created: { time: 2000 },
                    } as any,
                }),
            ];

            const originalOrder = documents.map((d) => d.urn);
            const result = sortDocumentsByCreationTime(documents);

            // Original array should be unchanged
            expect(documents.map((d) => d.urn)).toEqual(originalOrder);
            // Result should be sorted
            expect(result.map((d) => d.urn)).toEqual(['urn:li:document:2', 'urn:li:document:1']);
        });

        it('should handle empty array', () => {
            const documents: Document[] = [];

            const result = sortDocumentsByCreationTime(documents);

            expect(result).toEqual([]);
        });

        it('should handle single document', () => {
            const documents: Document[] = [
                createTestDocument({
                    urn: 'urn:li:document:1',
                    info: {
                        title: 'Single Doc',
                        created: { time: 1000 },
                    } as any,
                }),
            ];

            const result = sortDocumentsByCreationTime(documents);

            expect(result).toHaveLength(1);
            expect(result[0].urn).toBe('urn:li:document:1');
        });

        it('should handle documents with same creation time', () => {
            const documents: Document[] = [
                createTestDocument({
                    urn: 'urn:li:document:1',
                    info: {
                        title: 'Doc 1',
                        created: { time: 1000 },
                    } as any,
                }),
                createTestDocument({
                    urn: 'urn:li:document:2',
                    info: {
                        title: 'Doc 2',
                        created: { time: 1000 },
                    } as any,
                }),
            ];

            const result = sortDocumentsByCreationTime(documents);

            // Both should be present, order may vary but both should be there
            expect(result).toHaveLength(2);
            expect(result.map((d) => d.urn)).toContain('urn:li:document:1');
            expect(result.map((d) => d.urn)).toContain('urn:li:document:2');
        });
    });

    describe('hasBalancedParens', () => {
        it('should return true for strings with balanced parentheses', () => {
            expect(hasBalancedParens('()')).toBe(true);
            expect(hasBalancedParens('(a,b,c)')).toBe(true);
            expect(hasBalancedParens('((nested))')).toBe(true);
            expect(hasBalancedParens('(a,(b,c),d)')).toBe(true);
        });

        it('should return true for strings without parentheses', () => {
            expect(hasBalancedParens('')).toBe(true);
            expect(hasBalancedParens('no parens here')).toBe(true);
        });

        it('should return false for unbalanced parentheses', () => {
            expect(hasBalancedParens('(')).toBe(false);
            expect(hasBalancedParens(')')).toBe(false);
            expect(hasBalancedParens('(()')).toBe(false);
            expect(hasBalancedParens('())')).toBe(false);
            expect(hasBalancedParens(')(')).toBe(false);
        });

        it('should handle complex nested URN patterns', () => {
            // Valid DataJob URN pattern
            expect(hasBalancedParens('(urn:li:dataFlow:(airflow,dag,PROD),task)')).toBe(true);
            // Missing closing paren (the bug we fixed)
            expect(hasBalancedParens('(urn:li:dataFlow:(airflow,dag,PROD),task')).toBe(false);
        });
    });

    describe('isAllowedRelatedAssetUrn', () => {
        it('should return true for allowed entity types', () => {
            expect(isAllowedRelatedAssetUrn('urn:li:dataset:foo')).toBe(true);
            expect(isAllowedRelatedAssetUrn('urn:li:dataJob:(urn:li:dataFlow:(a,b,c),task)')).toBe(true);
            expect(isAllowedRelatedAssetUrn('urn:li:dashboard:123')).toBe(true);
            expect(isAllowedRelatedAssetUrn('urn:li:glossaryTerm:term1')).toBe(true);
        });

        it('should return false for disallowed entity types', () => {
            expect(isAllowedRelatedAssetUrn('urn:li:corpuser:john')).toBe(false);
            expect(isAllowedRelatedAssetUrn('urn:li:corpgroup:engineering')).toBe(false);
            expect(isAllowedRelatedAssetUrn('urn:li:structuredProperty:prop1')).toBe(false);
            expect(isAllowedRelatedAssetUrn('urn:li:dataPlatform:mysql')).toBe(false);
        });

        it('should return false for malformed URNs', () => {
            expect(isAllowedRelatedAssetUrn('not-a-urn')).toBe(false);
            expect(isAllowedRelatedAssetUrn('urn:li')).toBe(false);
            // Unbalanced parens
            expect(isAllowedRelatedAssetUrn('urn:li:dataJob:(urn:li:dataFlow:(a,b,c),task')).toBe(false);
        });
    });

    describe('extractUrnsFromMarkdown', () => {
        it('should extract simple URNs from markdown links', () => {
            const content = '[@Dataset](urn:li:dataset:foo)';
            const result = extractUrnsFromMarkdown(content);
            expect(result).toEqual(['urn:li:dataset:foo']);
        });

        it('should extract URNs with nested parentheses (DataJob pattern)', () => {
            const content = '[@Task](urn:li:dataJob:(urn:li:dataFlow:(airflow,dag,PROD),mytask))';
            const result = extractUrnsFromMarkdown(content);
            expect(result).toEqual(['urn:li:dataJob:(urn:li:dataFlow:(airflow,dag,PROD),mytask)']);
        });

        it('should extract multiple URNs from content', () => {
            const content =
                'See [@Dataset](urn:li:dataset:a) and [@Task](urn:li:dataJob:(urn:li:dataFlow:(x,y,z),task))';
            const result = extractUrnsFromMarkdown(content);
            expect(result).toEqual(['urn:li:dataset:a', 'urn:li:dataJob:(urn:li:dataFlow:(x,y,z),task)']);
        });

        it('should not include duplicate URNs', () => {
            const content = '[@A](urn:li:dataset:foo) and [@B](urn:li:dataset:foo)';
            const result = extractUrnsFromMarkdown(content);
            expect(result).toEqual(['urn:li:dataset:foo']);
        });

        it('should ignore non-URN links', () => {
            const content = '[Link](https://example.com) and [@Dataset](urn:li:dataset:foo)';
            const result = extractUrnsFromMarkdown(content);
            expect(result).toEqual(['urn:li:dataset:foo']);
        });

        it('should return empty array for content without URN links', () => {
            const content = 'Just some text without any links';
            const result = extractUrnsFromMarkdown(content);
            expect(result).toEqual([]);
        });
    });
});
