import { describe, expect, it } from 'vitest';

import { DocumentTreeNode } from '@app/document/DocumentTreeContext';
import { formatPlatformLabel, partitionRootNodesByLayer } from '@app/document/utils/documentTreeGrouping';

import { DataPlatform } from '@types';

const NOTION = { urn: 'urn:li:dataPlatform:notion', name: 'notion' } as DataPlatform;
const GITHUB = { urn: 'urn:li:dataPlatform:github', name: 'github' } as DataPlatform;
const CONFLUENCE = { urn: 'urn:li:dataPlatform:confluence', name: 'confluence' } as DataPlatform;
const DATAHUB = { urn: 'urn:li:dataPlatform:datahub', name: 'datahub' } as DataPlatform;

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

    it('places DataHub-platform nodes into the native bucket', () => {
        const native1 = makeNode({ urn: 'a', platform: DATAHUB });
        const native2 = makeNode({ urn: 'b', platform: DATAHUB });

        const result = partitionRootNodesByLayer([native1, native2]);

        expect(result.native).toEqual([native1, native2]);
        expect(result.sourcesByPlatform).toEqual([]);
    });

    it('places nodes with no platform into the native bucket', () => {
        const noPlatform = makeNode({ urn: 'orphan', platform: null });

        const result = partitionRootNodesByLayer([noPlatform]);

        expect(result.native).toEqual([noPlatform]);
        expect(result.sourcesByPlatform).toEqual([]);
    });

    it('groups nodes by resolved platform into per-platform source sections', () => {
        const notionDoc = makeNode({ urn: 'n1', platform: NOTION });
        const githubDoc1 = makeNode({ urn: 'g1', platform: GITHUB });
        const githubDoc2 = makeNode({ urn: 'g2', platform: GITHUB });

        const result = partitionRootNodesByLayer([githubDoc1, notionDoc, githubDoc2]);

        expect(result.native).toEqual([]);
        expect(result.sourcesByPlatform).toEqual([
            { platform: GITHUB, label: 'Github', nodes: [githubDoc1, githubDoc2] },
            { platform: NOTION, label: 'Notion', nodes: [notionDoc] },
        ]);
    });

    it('groups by platform independent of NATIVE/EXTERNAL source type', () => {
        // A GitHub doc imported as NATIVE still resolves platform=github, so it
        // lands in the GitHub source section, not the DataHub section.
        const nativeGithubDoc = makeNode({ urn: 'g-native', isExternal: false, platform: GITHUB });
        const externalGithubDoc = makeNode({ urn: 'g-ext', isExternal: true, platform: GITHUB });

        const result = partitionRootNodesByLayer([nativeGithubDoc, externalGithubDoc]);

        expect(result.native).toEqual([]);
        expect(result.sourcesByPlatform).toEqual([
            { platform: GITHUB, label: 'Github', nodes: [nativeGithubDoc, externalGithubDoc] },
        ]);
    });

    it('preserves first-appearance order across platform groups', () => {
        const confluenceDoc = makeNode({ urn: 'c1', platform: CONFLUENCE });
        const githubDoc = makeNode({ urn: 'g1', platform: GITHUB });
        const notionDoc = makeNode({ urn: 'n1', platform: NOTION });

        const result = partitionRootNodesByLayer([confluenceDoc, githubDoc, notionDoc]);

        expect(result.sourcesByPlatform.map((g) => g.platform)).toEqual([CONFLUENCE, GITHUB, NOTION]);
    });

    it('separates the DataHub and source layers when both are present', () => {
        const native = makeNode({ urn: 'native', platform: DATAHUB });
        const external = makeNode({ urn: 'external', platform: GITHUB });

        const result = partitionRootNodesByLayer([native, external]);

        expect(result.native).toEqual([native]);
        expect(result.sourcesByPlatform).toEqual([{ platform: GITHUB, label: 'Github', nodes: [external] }]);
    });

    it('falls back to the native bucket when the platform name is unresolvable', () => {
        // Non-DataHub platform but no name/displayName to label a section with.
        const unlabeledPlatform = { urn: 'urn:li:dataPlatform:mystery' } as DataPlatform;
        const node = makeNode({ urn: 'mystery', platform: unlabeledPlatform });

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
