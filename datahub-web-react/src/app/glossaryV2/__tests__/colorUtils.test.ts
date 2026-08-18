import {
    getDeepestParentNode,
    getGlossaryTermColor,
    getStringHash,
    resolveGlossaryEntityColor,
} from '@app/glossaryV2/colorUtils';

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

    it("prefers the term's own displayProperties.colorHex above any parent color", () => {
        // Term wins even when both a deeper colorHex-bearing parent and a palette fallback are
        // available — once a user has picked a color via the term's picker, that's authoritative.
        const TERM_COLOR = 'term-own-color';
        const PARENT_COLOR = 'parent-display-color';
        const term = {
            urn: 'urn:li:glossaryTerm:t1',
            displayProperties: { colorHex: TERM_COLOR },
            parentNodes: makeParentNodes([
                makeParentNode('urn:li:glossaryNode:direct'),
                makeParentNode('urn:li:glossaryNode:root', PARENT_COLOR),
            ]),
        } as unknown as GlossaryTerm;
        expect(getGlossaryTermColor(term, generateColor)).toBe(TERM_COLOR);
    });

    it("uses the term's own displayProperties.colorHex for a root-level term", () => {
        const TERM_COLOR = 'term-own-color';
        const term = {
            urn: 'urn:li:glossaryTerm:root-level',
            displayProperties: { colorHex: TERM_COLOR },
            parentNodes: undefined,
        } as unknown as GlossaryTerm;
        expect(getGlossaryTermColor(term, generateColor)).toBe(TERM_COLOR);
    });

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

describe('resolveGlossaryEntityColor', () => {
    const generateColor = (urn: string) => `palette-${urn}`;

    it("returns the entity's own colorHex above every other source", () => {
        // Own colorHex beats inheritedColor AND a colored root parent. Used by the sidebar +
        // entity header so a user-picked color is always visible wherever the entity appears.
        const own = {
            urn: 'urn:li:glossaryNode:descendant',
            displayProperties: { colorHex: 'descendant-own' },
            parentNodes: makeParentNodes([makeParentNode('urn:li:glossaryNode:root', 'root-color')]),
        } as unknown as GlossaryNode;
        expect(resolveGlossaryEntityColor(own, generateColor, { inheritedColor: 'sidebar-inherited' })).toBe(
            'descendant-own',
        );
    });

    it('uses inheritedColor when the entity has no explicit colorHex (sidebar recursion path)', () => {
        // `NodeItem` passes its resolved color down to its children as `iconColor`; the resolver
        // folds that into the precedence chain BEFORE deriving from parentNodes so deeply-nested
        // descendants in the sidebar paint the same color as their topmost rendered ancestor.
        const child = {
            urn: 'urn:li:glossaryNode:child',
            parentNodes: makeParentNodes([makeParentNode('urn:li:glossaryNode:root')]),
        } as unknown as GlossaryNode;
        expect(resolveGlossaryEntityColor(child, generateColor, { inheritedColor: 'sidebar-inherited' })).toBe(
            'sidebar-inherited',
        );
    });

    it('derives from the deepest parent when neither own colorHex nor inheritedColor are present', () => {
        // The autocomplete + flat list cards don't pass `inheritedColor`. The resolver should
        // fall back to the deepest (root) parent's color so a term still reads as part of its
        // group's identity.
        const term = {
            urn: 'urn:li:glossaryTerm:t1',
            parentNodes: makeParentNodes([
                makeParentNode('urn:li:glossaryNode:direct'),
                makeParentNode('urn:li:glossaryNode:root', 'root-color'),
            ]),
        } as unknown as GlossaryTerm;
        expect(resolveGlossaryEntityColor(term, generateColor)).toBe('root-color');
    });

    it("falls back to the entity's own URN palette when no parents are present", () => {
        // Root entities have no parents; the palette slot is seeded from their own URN so two
        // distinct root entities never collide on the same color slot by accident.
        const rootNode = {
            urn: 'urn:li:glossaryNode:root',
            parentNodes: undefined,
        } as unknown as GlossaryNode;
        expect(resolveGlossaryEntityColor(rootNode, generateColor)).toBe('palette-urn:li:glossaryNode:root');
    });

    it('honors own colorHex even on a root entity (no parents, no inheritedColor)', () => {
        const rootWithOwn = {
            urn: 'urn:li:glossaryNode:root',
            displayProperties: { colorHex: 'root-own' },
            parentNodes: undefined,
        } as unknown as GlossaryNode;
        expect(resolveGlossaryEntityColor(rootWithOwn, generateColor)).toBe('root-own');
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
