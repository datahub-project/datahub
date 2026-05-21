import { getDeepestParentNode, getGlossaryTermColor, getStringHash } from '@app/glossaryV2/colorUtils';

import { EntityType, GlossaryNode, GlossaryTerm, ParentNodesResult } from '@types';

const makeParentNode = (urn: string, colorHex?: string): GlossaryNode =>
    ({
        urn,
        type: EntityType.GlossaryNode,
        displayProperties: colorHex ? { colorHex } : undefined,
    }) as unknown as GlossaryNode;

const makeParentNodes = (nodes: GlossaryNode[]): ParentNodesResult => ({
    count: nodes.length,
    nodes,
});

describe('getDeepestParentNode', () => {
    it('returns null when parentNodes is undefined', () => {
        expect(getDeepestParentNode(undefined)).toBeNull();
    });

    it('returns null when parentNodes is null', () => {
        expect(getDeepestParentNode(null)).toBeNull();
    });

    it('returns null when parentNodes has zero count', () => {
        expect(getDeepestParentNode({ count: 0, nodes: [] })).toBeNull();
    });

    it('returns the last node (deepest ancestor) — GraphQL orders direct-parent → root', () => {
        const direct = makeParentNode('urn:li:glossaryNode:direct');
        const root = makeParentNode('urn:li:glossaryNode:root');
        expect(getDeepestParentNode(makeParentNodes([direct, root]))).toBe(root);
    });
});

describe('getGlossaryTermColor', () => {
    const generateColor = (urn: string) => `palette-${urn}`;

    it('uses the root parent node displayProperties.colorHex when set', () => {
        // Function returns the colorHex string verbatim — any sentinel works, no need for a real hex.
        const PARENT_COLOR = 'parent-display-color';
        const term = {
            urn: 'urn:li:glossaryTerm:t1',
            parentNodes: makeParentNodes([
                makeParentNode('urn:li:glossaryNode:direct'),
                makeParentNode('urn:li:glossaryNode:root', PARENT_COLOR),
            ]),
        } as unknown as GlossaryTerm;
        expect(getGlossaryTermColor(term, generateColor)).toBe(PARENT_COLOR);
    });

    it('derives color from the root parent URN when no colorHex on the parent', () => {
        const term = {
            urn: 'urn:li:glossaryTerm:t1',
            parentNodes: makeParentNodes([
                makeParentNode('urn:li:glossaryNode:direct'),
                makeParentNode('urn:li:glossaryNode:root'),
            ]),
        } as unknown as GlossaryTerm;
        expect(getGlossaryTermColor(term, generateColor)).toBe('palette-urn:li:glossaryNode:root');
    });

    it('falls back to the term URN when there are no parent nodes', () => {
        const term = {
            urn: 'urn:li:glossaryTerm:root-level',
            parentNodes: undefined,
        } as unknown as GlossaryTerm;
        expect(getGlossaryTermColor(term, generateColor)).toBe('palette-urn:li:glossaryTerm:root-level');
    });

    it('falls back to the term URN when parentNodes has zero count', () => {
        const term = {
            urn: 'urn:li:glossaryTerm:root-level',
            parentNodes: { count: 0, nodes: [] },
        } as unknown as GlossaryTerm;
        expect(getGlossaryTermColor(term, generateColor)).toBe('palette-urn:li:glossaryTerm:root-level');
    });
});

describe('getStringHash', () => {
    it('returns the same hash for the same input (deterministic)', () => {
        expect(getStringHash('urn:li:glossaryTerm:abc')).toBe(getStringHash('urn:li:glossaryTerm:abc'));
    });

    it('returns 0 for an empty string', () => {
        expect(getStringHash('')).toBe(0);
    });
});
