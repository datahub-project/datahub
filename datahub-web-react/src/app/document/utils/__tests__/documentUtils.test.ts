import { describe, expect, it } from 'vitest';

import {
    computeRelatedEntitiesForLinkChange,
    documentToTreeNode,
    extractDocumentCreator,
    extractRelatedDocumentUrns,
    extractUrnsFromMarkdown,
    hasBalancedParens,
    isAllowedRelatedAssetUrn,
    isDocumentUnpublished,
    isExternalDocument,
    resolveActorDisplayName,
    sortDocumentsByCreationTime,
} from '@app/document/utils/documentUtils';

import { DataPlatform, Document, DocumentSourceType, DocumentState, EntityType } from '@types';

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
                isUnpublished: false, // No status state on the fixture → defaults to published
                isExternal: false, // No info.source on the fixture → defaults to native
                platform: null, // No platform on the fixture
                creator: null, // No created actor on the fixture
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

        it('should mark isUnpublished true when info.status.state is UNPUBLISHED', () => {
            const unpublishedDoc = createTestDocument({
                info: { status: { state: DocumentState.Unpublished } } as any,
            });
            const publishedDoc = createTestDocument({
                info: { status: { state: DocumentState.Published } } as any,
            });

            expect(documentToTreeNode(unpublishedDoc, false).isUnpublished).toBe(true);
            expect(documentToTreeNode(publishedDoc, false).isUnpublished).toBe(false);
        });

        it('should carry isExternal and platform through to the tree node', () => {
            const platform = {
                urn: 'urn:li:dataPlatform:notion',
                type: EntityType.DataPlatform,
                properties: { logoUrl: 'http://example.com/notion.png' },
            } as DataPlatform;
            const externalDoc = createTestDocument({
                platform,
                info: { source: { sourceType: DocumentSourceType.External } } as any,
            });
            const nativeDoc = createTestDocument({
                platform,
                info: { source: { sourceType: DocumentSourceType.Native } } as any,
            });

            const externalNode = documentToTreeNode(externalDoc, false);
            expect(externalNode.isExternal).toBe(true);
            expect(externalNode.platform).toEqual(platform);

            const nativeNode = documentToTreeNode(nativeDoc, false);
            expect(nativeNode.isExternal).toBe(false);
            // Platform is still carried even on native docs — it's just not
            // consumed visually when isExternal is false.
            expect(nativeNode.platform).toEqual(platform);
        });

        it('should populate creator from the created actor', () => {
            const doc = createTestDocument({
                info: {
                    created: {
                        actor: {
                            urn: 'urn:li:corpuser:jane',
                            type: EntityType.CorpUser,
                            properties: { displayName: 'Jane Doe' },
                            editableProperties: { pictureLink: 'http://example.com/jane.png' },
                        },
                    },
                } as any,
            });

            const result = documentToTreeNode(doc, false);
            expect(result.creator).toEqual({
                urn: 'urn:li:corpuser:jane',
                type: EntityType.CorpUser,
                displayName: 'Jane Doe',
                pictureLink: 'http://example.com/jane.png',
            });
        });
    });

    describe('resolveActorDisplayName', () => {
        it('should prefer editableProperties.displayName over everything else', () => {
            expect(
                resolveActorDisplayName({
                    urn: 'urn:li:corpuser:jane',
                    editableProperties: { displayName: 'Jane the Override' },
                    properties: { displayName: 'Jane the Canonical', fullName: 'Jane Doe' },
                    info: { displayName: 'Jane the Legacy' },
                    username: 'jane',
                }),
            ).toBe('Jane the Override');
        });

        it('should fall back through properties.displayName, info.displayName, fullName, firstName+lastName, username, name, urn', () => {
            expect(resolveActorDisplayName({ properties: { displayName: 'Canonical' } })).toBe('Canonical');
            expect(resolveActorDisplayName({ info: { displayName: 'Legacy' } })).toBe('Legacy');
            expect(resolveActorDisplayName({ properties: { fullName: 'Jane Doe' } })).toBe('Jane Doe');
            expect(resolveActorDisplayName({ properties: { firstName: 'Jane', lastName: 'Doe' } })).toBe('Jane Doe');
            expect(resolveActorDisplayName({ username: 'jane' })).toBe('jane');
            expect(resolveActorDisplayName({ name: 'engineering' })).toBe('engineering');
            expect(resolveActorDisplayName({ urn: 'urn:li:corpuser:jane' })).toBe('urn:li:corpuser:jane');
        });

        it('should return an empty string for null/undefined actor', () => {
            expect(resolveActorDisplayName(null)).toBe('');
            expect(resolveActorDisplayName(undefined)).toBe('');
        });
    });

    describe('extractDocumentCreator', () => {
        it('should return null when no actor is present', () => {
            expect(extractDocumentCreator(null)).toBeNull();
            expect(extractDocumentCreator(undefined)).toBeNull();
            expect(extractDocumentCreator({})).toBeNull();
            expect(extractDocumentCreator({ info: null })).toBeNull();
            expect(extractDocumentCreator({ info: { created: null } })).toBeNull();
            expect(extractDocumentCreator({ info: { created: { actor: null } } })).toBeNull();
        });

        it('should return null when the actor lacks urn or type', () => {
            expect(extractDocumentCreator({ info: { created: { actor: { urn: 'urn:li:corpuser:x' } } } })).toBeNull();
            expect(extractDocumentCreator({ info: { created: { actor: { type: EntityType.CorpUser } } } })).toBeNull();
        });

        it('should resolve displayName and pictureLink', () => {
            expect(
                extractDocumentCreator({
                    info: {
                        created: {
                            actor: {
                                urn: 'urn:li:corpuser:jane',
                                type: EntityType.CorpUser,
                                properties: { displayName: 'Jane Doe' },
                                editableProperties: { pictureLink: 'http://example.com/jane.png' },
                            },
                        },
                    },
                }),
            ).toEqual({
                urn: 'urn:li:corpuser:jane',
                type: EntityType.CorpUser,
                displayName: 'Jane Doe',
                pictureLink: 'http://example.com/jane.png',
            });
        });

        it('should default pictureLink to null when not provided', () => {
            const result = extractDocumentCreator({
                info: {
                    created: {
                        actor: {
                            urn: 'urn:li:corpuser:jane',
                            type: EntityType.CorpUser,
                            properties: { displayName: 'Jane Doe' },
                        },
                    },
                },
            });
            expect(result?.pictureLink).toBeNull();
        });
    });

    describe('isDocumentUnpublished', () => {
        it('should return true for UNPUBLISHED state', () => {
            expect(isDocumentUnpublished({ info: { status: { state: DocumentState.Unpublished } } })).toBe(true);
        });

        it('should return false for PUBLISHED state', () => {
            expect(isDocumentUnpublished({ info: { status: { state: DocumentState.Published } } })).toBe(false);
        });

        it('should return false when status info is missing', () => {
            expect(isDocumentUnpublished(null)).toBe(false);
            expect(isDocumentUnpublished(undefined)).toBe(false);
            expect(isDocumentUnpublished({})).toBe(false);
            expect(isDocumentUnpublished({ info: null })).toBe(false);
            expect(isDocumentUnpublished({ info: { status: null } })).toBe(false);
            expect(isDocumentUnpublished({ info: { status: { state: null } } })).toBe(false);
        });
    });

    describe('isExternalDocument', () => {
        it('should return true when source.sourceType is EXTERNAL', () => {
            expect(isExternalDocument({ info: { source: { sourceType: DocumentSourceType.External } } })).toBe(true);
        });

        it('should return false when source.sourceType is NATIVE', () => {
            expect(isExternalDocument({ info: { source: { sourceType: DocumentSourceType.Native } } })).toBe(false);
        });

        it('should return false when source or sourceType is missing', () => {
            expect(isExternalDocument(null)).toBe(false);
            expect(isExternalDocument(undefined)).toBe(false);
            expect(isExternalDocument({})).toBe(false);
            expect(isExternalDocument({ info: null })).toBe(false);
            expect(isExternalDocument({ info: { source: null } })).toBe(false);
            expect(isExternalDocument({ info: { source: { sourceType: null } } })).toBe(false);
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
            expect(isAllowedRelatedAssetUrn('urn:li:container:abc')).toBe(true);
            expect(isAllowedRelatedAssetUrn('urn:li:tag:myTag')).toBe(true);
        });

        it('should return false for entity types not in the allow list', () => {
            expect(isAllowedRelatedAssetUrn('urn:li:corpuser:john')).toBe(false);
            expect(isAllowedRelatedAssetUrn('urn:li:corpgroup:engineering')).toBe(false);
            expect(isAllowedRelatedAssetUrn('urn:li:structuredProperty:prop1')).toBe(false);
            expect(isAllowedRelatedAssetUrn('urn:li:dataPlatform:mysql')).toBe(false);
            expect(isAllowedRelatedAssetUrn('urn:li:schemaField:abc')).toBe(false);
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

    describe('extractRelatedDocumentUrns', () => {
        it('should extract related document URNs', () => {
            const document = {
                info: {
                    relatedDocuments: [
                        { document: { urn: 'urn:li:document:a' } },
                        { document: { urn: 'urn:li:document:b' } },
                    ],
                },
            };
            expect(extractRelatedDocumentUrns(document)).toEqual(['urn:li:document:a', 'urn:li:document:b']);
        });

        it('should return empty array when there are no related documents', () => {
            expect(extractRelatedDocumentUrns({ info: { relatedDocuments: [] } })).toEqual([]);
            expect(extractRelatedDocumentUrns({ info: {} })).toEqual([]);
            expect(extractRelatedDocumentUrns(null)).toEqual([]);
        });

        it('should skip entries with missing document or urn', () => {
            const document = {
                info: {
                    relatedDocuments: [{ document: { urn: 'urn:li:document:a' } }, { document: null }],
                },
            };
            expect(extractRelatedDocumentUrns(document)).toEqual(['urn:li:document:a']);
        });
    });

    describe('computeRelatedEntitiesForLinkChange', () => {
        const entityUrn = 'urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)';
        const docUrn = 'urn:li:document:parent';

        it('adds a normal entity to relatedAssets and leaves relatedDocuments untouched', () => {
            expect(
                computeRelatedEntitiesForLinkChange({
                    entityUrn,
                    existingAssetUrns: ['urn:li:dataset:other'],
                    existingRelatedDocumentUrns: ['urn:li:document:keep'],
                    shouldBeLinked: true,
                }),
            ).toEqual({
                relatedAssets: ['urn:li:dataset:other', entityUrn],
                relatedDocuments: ['urn:li:document:keep'],
            });
        });

        it('removes a normal entity from relatedAssets only', () => {
            expect(
                computeRelatedEntitiesForLinkChange({
                    entityUrn,
                    existingAssetUrns: ['urn:li:dataset:other', entityUrn],
                    existingRelatedDocumentUrns: ['urn:li:document:keep'],
                    shouldBeLinked: false,
                }),
            ).toEqual({
                relatedAssets: ['urn:li:dataset:other'],
                relatedDocuments: ['urn:li:document:keep'],
            });
        });

        it('routes a document URN to relatedDocuments and leaves relatedAssets untouched', () => {
            expect(
                computeRelatedEntitiesForLinkChange({
                    entityUrn: docUrn,
                    existingAssetUrns: ['urn:li:dataset:keep'],
                    existingRelatedDocumentUrns: [],
                    shouldBeLinked: true,
                }),
            ).toEqual({
                relatedAssets: ['urn:li:dataset:keep'],
                relatedDocuments: [docUrn],
            });
        });

        it('does not duplicate an already-linked entity', () => {
            const result = computeRelatedEntitiesForLinkChange({
                entityUrn,
                existingAssetUrns: [entityUrn],
                existingRelatedDocumentUrns: [],
                shouldBeLinked: true,
            });
            expect(result.relatedAssets).toEqual([entityUrn]);
        });
    });
});
