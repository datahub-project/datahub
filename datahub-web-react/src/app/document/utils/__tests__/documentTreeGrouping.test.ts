import { describe, expect, it } from 'vitest';

import { DocumentTreeNode } from '@app/document/DocumentTreeContext';
import { partitionRootNodesByLayer } from '@app/document/utils/documentTreeGrouping';

import { DataPlatform } from '@types';

const NOTION = { urn: 'urn:li:dataPlatform:notion' } as DataPlatform;
const GITHUB = { urn: 'urn:li:dataPlatform:github' } as DataPlatform;
const DATAHUB = { urn: 'urn:li:dataPlatform:datahub' } as DataPlatform;

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
    it('returns empty native and other buckets for an empty input', () => {
        expect(partitionRootNodesByLayer([])).toEqual({
            native: [],
            other: [],
        });
    });

    it('places DataHub-platform nodes into the native bucket', () => {
        const native1 = makeNode({ urn: 'a', platform: DATAHUB });
        const native2 = makeNode({ urn: 'b', platform: DATAHUB });

        const result = partitionRootNodesByLayer([native1, native2]);

        expect(result.native).toEqual([native1, native2]);
        expect(result.other).toEqual([]);
    });

    it('places nodes with no platform into the native bucket', () => {
        const noPlatform = makeNode({ urn: 'orphan', platform: null });

        const result = partitionRootNodesByLayer([noPlatform]);

        expect(result.native).toEqual([noPlatform]);
        expect(result.other).toEqual([]);
    });

    it('collapses all non-DataHub platforms into the single other bucket', () => {
        const notionDoc = makeNode({ urn: 'n1', platform: NOTION });
        const githubDoc1 = makeNode({ urn: 'g1', platform: GITHUB });
        const githubDoc2 = makeNode({ urn: 'g2', platform: GITHUB });

        const result = partitionRootNodesByLayer([githubDoc1, notionDoc, githubDoc2]);

        expect(result.native).toEqual([]);
        expect(result.other).toEqual([githubDoc1, notionDoc, githubDoc2]);
    });

    it('buckets by platform independent of NATIVE/EXTERNAL source type', () => {
        // A GitHub doc imported as NATIVE still has platform=github, so it lands
        // in "Other" rather than the DataHub section.
        const nativeGithubDoc = makeNode({ urn: 'g-native', isExternal: false, platform: GITHUB });
        const externalGithubDoc = makeNode({ urn: 'g-ext', isExternal: true, platform: GITHUB });

        const result = partitionRootNodesByLayer([nativeGithubDoc, externalGithubDoc]);

        expect(result.native).toEqual([]);
        expect(result.other).toEqual([nativeGithubDoc, externalGithubDoc]);
    });

    it('separates the DataHub and other layers when both are present', () => {
        const native = makeNode({ urn: 'native', platform: DATAHUB });
        const external = makeNode({ urn: 'external', platform: GITHUB });

        const result = partitionRootNodesByLayer([native, external]);

        expect(result.native).toEqual([native]);
        expect(result.other).toEqual([external]);
    });

    it('preserves first-appearance order within the other bucket', () => {
        const notionDoc = makeNode({ urn: 'n1', platform: NOTION });
        const githubDoc = makeNode({ urn: 'g1', platform: GITHUB });

        const result = partitionRootNodesByLayer([notionDoc, githubDoc]);

        expect(result.other).toEqual([notionDoc, githubDoc]);
    });
});
