import { describe, expect, it } from 'vitest';

import { DocumentTreeNode } from '@app/document/DocumentTreeContext';
import { formatPlatformLabel, partitionRootNodesByLayer } from '@app/document/utils/documentTreeGrouping';

import { DataPlatform } from '@types';

const NOTION = { urn: 'urn:li:dataPlatform:notion' } as DataPlatform;
const GITHUB = { urn: 'urn:li:dataPlatform:github' } as DataPlatform;
const CONFLUENCE = { urn: 'urn:li:dataPlatform:confluence' } as DataPlatform;

function makeNode(overrides: Partial<DocumentTreeNode> = {}): DocumentTreeNode {
    return {
        urn: 'urn:li:document:test',
        title: 'Test Document',
        parentUrn: null,
        hasChildren: false,
        children: undefined,
        isUnpublished: false,
        isExternal: false,
        platform: null,
        ...overrides,
    };
}

describe('partitionRootNodesByLayer', () => {
    it('returns empty native and sources buckets for an empty input', () => {
        expect(partitionRootNodesByLayer([])).toEqual({
            native: [],
            sourcesByPlatform: [],
        });
    });

    it('places non-external nodes into the native bucket', () => {
        const native1 = makeNode({ urn: 'a', isExternal: false });
        const native2 = makeNode({ urn: 'b', isExternal: false });

        const result = partitionRootNodesByLayer([native1, native2]);

        expect(result.native).toEqual([native1, native2]);
        expect(result.sourcesByPlatform).toEqual([]);
    });

    it('groups external nodes by platform URN', () => {
        const notionDoc = makeNode({ urn: 'n1', isExternal: true, platform: NOTION });
        const githubDoc1 = makeNode({ urn: 'g1', isExternal: true, platform: GITHUB });
        const githubDoc2 = makeNode({ urn: 'g2', isExternal: true, platform: GITHUB });

        const result = partitionRootNodesByLayer([githubDoc1, notionDoc, githubDoc2]);

        expect(result.native).toEqual([]);
        expect(result.sourcesByPlatform).toEqual([
            { platform: GITHUB, nodes: [githubDoc1, githubDoc2] },
            { platform: NOTION, nodes: [notionDoc] },
        ]);
    });

    it('preserves first-appearance order across platform groups', () => {
        const confluenceDoc = makeNode({ urn: 'c1', isExternal: true, platform: CONFLUENCE });
        const githubDoc = makeNode({ urn: 'g1', isExternal: true, platform: GITHUB });
        const notionDoc = makeNode({ urn: 'n1', isExternal: true, platform: NOTION });

        const result = partitionRootNodesByLayer([confluenceDoc, githubDoc, notionDoc]);

        expect(result.sourcesByPlatform.map((g) => g.platform)).toEqual([CONFLUENCE, GITHUB, NOTION]);
    });

    it('separates the native and sources layers when both are present', () => {
        const native = makeNode({ urn: 'native', isExternal: false });
        const external = makeNode({ urn: 'external', isExternal: true, platform: GITHUB });

        const result = partitionRootNodesByLayer([native, external]);

        expect(result.native).toEqual([native]);
        expect(result.sourcesByPlatform).toEqual([{ platform: GITHUB, nodes: [external] }]);
    });

    it('drops external nodes that have no platform', () => {
        const noPlatform = makeNode({ urn: 'orphan', isExternal: true, platform: null });
        const withPlatform = makeNode({ urn: 'gh', isExternal: true, platform: GITHUB });

        const result = partitionRootNodesByLayer([noPlatform, withPlatform]);

        expect(result.sourcesByPlatform).toEqual([{ platform: GITHUB, nodes: [withPlatform] }]);
    });

    it('treats nodes with missing isExternal/platform as native', () => {
        const node = makeNode({ urn: 'legacy' });
        const result = partitionRootNodesByLayer([node]);

        expect(result.native).toEqual([node]);
        expect(result.sourcesByPlatform).toEqual([]);
    });
});

describe('formatPlatformLabel', () => {
    it('prefers properties.displayName when present', () => {
        const platform = { name: 'confluence', properties: { displayName: 'Confluence' } } as DataPlatform;
        expect(formatPlatformLabel(platform)).toBe('Confluence');
    });

    it('title-cases snake_case names when displayName is missing', () => {
        const platform = { name: 'google_docs' } as DataPlatform;
        expect(formatPlatformLabel(platform)).toBe('Google Docs');
    });

    it('capitalizes a single-word name', () => {
        const platform = { name: 'confluence' } as DataPlatform;
        expect(formatPlatformLabel(platform)).toBe('Confluence');
    });

    it('returns an empty string when neither displayName nor name is set', () => {
        const platform = {} as DataPlatform;
        expect(formatPlatformLabel(platform)).toBe('');
    });
});
