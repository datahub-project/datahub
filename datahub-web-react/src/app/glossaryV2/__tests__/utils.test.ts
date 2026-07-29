import {
    ROOT_NODES,
    ROOT_TERMS,
    buildOptimisticGlossaryEntity,
    deriveGlossaryLabelFromUrn,
    getCollapsedGlossaryItems,
    getGlossaryEntityIcon,
    getGlossaryRootToUpdate,
    getParentNodeToUpdate,
    updateGlossarySidebar,
} from '@app/glossaryV2/utils';
import { glossaryNode1, glossaryNode3, glossaryTerm1 } from '@src/Mocks';

import { EntityType, GlossaryNode, GlossaryTerm } from '@types';

const glossaryTermWithParent = {
    ...glossaryTerm1,
    parentNodes: {
        count: 1,
        nodes: [glossaryNode1],
    },
};

describe('glossary utils tests', () => {
    it('should get the direct parent node urn in getParentNodeToUpdate for glossary nodes', () => {
        const parentNode = getParentNodeToUpdate(glossaryNode3 as any, EntityType.GlossaryNode);
        expect(parentNode).toBe(glossaryNode3.parentNodes?.nodes[0].urn);
    });

    it('should get the direct parent node urn in getParentNodeToUpdate for glossary terms', () => {
        const parentNode = getParentNodeToUpdate(glossaryTermWithParent as any, EntityType.GlossaryTerm);
        expect(parentNode).toBe(glossaryNode1.urn);
    });

    it('should return ROOT_NODES for glossary node with no parent nodes', () => {
        const parentNode = getParentNodeToUpdate(glossaryNode1 as any, EntityType.GlossaryNode);
        expect(parentNode).toBe(ROOT_NODES);
    });

    it('should return ROOT_TERMS for glossary term with no parent nodes', () => {
        const parentNode = getParentNodeToUpdate(glossaryTerm1 as any, EntityType.GlossaryTerm);
        expect(parentNode).toBe(ROOT_TERMS);
    });

    it('should return ROOT_NODES for glossary node', () => {
        expect(getGlossaryRootToUpdate(EntityType.GlossaryNode)).toBe(ROOT_NODES);
    });

    it('should return ROOT_TERMS for glossary term', () => {
        expect(getGlossaryRootToUpdate(EntityType.GlossaryTerm)).toBe(ROOT_TERMS);
    });

    it('should return updateGlossarySidebar for glossary', () => {
        const parentNodesToUpdate = ['sampleParentNode1', 'sampleParentNode2'];
        const urnsToUpdate = ['urnsSample1'];
        const setUrnsToUpdate = vi.fn();
        updateGlossarySidebar(parentNodesToUpdate, urnsToUpdate, setUrnsToUpdate);
        expect(setUrnsToUpdate).toHaveBeenCalledWith([...urnsToUpdate, ...parentNodesToUpdate]);
    });

    describe('buildOptimisticGlossaryEntity', () => {
        it("omits displayProperties when the user didn't pick a color (search-index fallback)", () => {
            // Without an explicit color, the sidebar should resolve through the inherited /
            // palette chain — same as the canonical server-side entry will. Persisting a null
            // colorHex stops the new entry from hard-coding the gray default in the sidebar.
            const result = buildOptimisticGlossaryEntity({
                urn: 'urn:li:glossaryTerm:new',
                entityType: EntityType.GlossaryTerm,
                name: 'New Term',
                description: null,
            });
            expect(result.displayProperties).toBeNull();
            expect(result.properties).toEqual({ name: 'New Term', description: null });
            expect(result.type).toBe(EntityType.GlossaryTerm);
        });

        it('records the picked color as displayProperties.colorHex', () => {
            const result = buildOptimisticGlossaryEntity({
                urn: 'urn:li:glossaryTerm:new',
                entityType: EntityType.GlossaryTerm,
                name: 'New Term',
                description: 'desc',
                colorHex: 'picked-color',
            });
            expect(result.displayProperties?.colorHex).toBe('picked-color');
        });

        it('synthesizes a direct-parent → root chain when a parent is provided', () => {
            // Mirrors the GraphQL parentNodes ordering (direct-parent first, then ancestors)
            // so `resolveGlossaryEntityColor` reads the same root for the optimistic entry
            // that it will for the canonical entry once the search index catches up — fixing
            // the brief color flash where a freshly-created child landed on a palette slot
            // derived from its own URN.
            const grandparent = {
                urn: 'urn:li:glossaryNode:grand',
                type: EntityType.GlossaryNode,
            } as unknown as GlossaryNode;
            const parent = {
                urn: 'urn:li:glossaryNode:direct',
                type: EntityType.GlossaryNode,
                parentNodes: { count: 1, nodes: [grandparent] },
            } as unknown as GlossaryNode;

            const result = buildOptimisticGlossaryEntity({
                urn: 'urn:li:glossaryTerm:new',
                entityType: EntityType.GlossaryTerm,
                name: 'New Term',
                parent,
            });
            expect(result.parentNodes?.count).toBe(2);
            expect(result.parentNodes?.nodes.map((n) => n.urn)).toEqual([parent.urn, grandparent.urn]);
        });

        it('returns null parentNodes when no parent is provided (root create)', () => {
            const result = buildOptimisticGlossaryEntity({
                urn: 'urn:li:glossaryNode:new-root',
                entityType: EntityType.GlossaryNode,
                name: 'New Root',
            });
            expect(result.parentNodes).toBeNull();
        });
    });

    describe('getCollapsedGlossaryItems', () => {
        const entityRegistry = {
            getDisplayName: (_type: EntityType, entity: unknown) =>
                (entity as { properties?: { name?: string }; urn: string }).properties?.name ??
                (entity as { urn: string }).urn,
        };
        const generateColor = (urn: string) => `palette-${urn}`;

        it('sorts each section by display name and concatenates nodes then terms', () => {
            // Nodes-then-terms ordering keeps the icon column visually grouped by entity type —
            // matches the expanded tree's "all nodes, then all terms" structure.
            const zNode = { urn: 'urn:n:z', type: EntityType.GlossaryNode, properties: { name: 'Zebra' } };
            const aNode = { urn: 'urn:n:a', type: EntityType.GlossaryNode, properties: { name: 'Apple' } };
            const bTerm = { urn: 'urn:t:b', type: EntityType.GlossaryTerm, properties: { name: 'Banana' } };
            const aTerm = { urn: 'urn:t:a', type: EntityType.GlossaryTerm, properties: { name: 'Avocado' } };
            const result = getCollapsedGlossaryItems({
                nodes: [zNode, aNode] as unknown as GlossaryNode[],
                terms: [bTerm, aTerm] as unknown as GlossaryTerm[],
                entityRegistry,
                generateColor,
            });
            expect(result.map((i) => i.name)).toEqual(['Apple', 'Zebra', 'Avocado', 'Banana']);
        });

        it("resolves color through the entity's own colorHex first, then palette fallback", () => {
            // Root-level entries have no parents, so the resolver chain reduces to
            // own colorHex → palette(own urn).
            const withColor = {
                urn: 'urn:n:withColor',
                type: EntityType.GlossaryNode,
                properties: { name: 'A' },
                displayProperties: { colorHex: 'explicit' },
            };
            const withoutColor = {
                urn: 'urn:n:withoutColor',
                type: EntityType.GlossaryNode,
                properties: { name: 'B' },
            };
            const result = getCollapsedGlossaryItems({
                nodes: [withColor, withoutColor] as unknown as GlossaryNode[],
                terms: [],
                entityRegistry,
                generateColor,
            });
            expect(result.find((i) => i.urn === 'urn:n:withColor')?.color).toBe('explicit');
            expect(result.find((i) => i.urn === 'urn:n:withoutColor')?.color).toBe('palette-urn:n:withoutColor');
        });

        it('pairs nodes with BookmarksSimple and terms with BookmarkSimple via getGlossaryEntityIcon', () => {
            // Routes both consumers through one icon picker so a future Phosphor swap only
            // touches `getGlossaryEntityIcon`.
            const node = { urn: 'urn:n:a', type: EntityType.GlossaryNode, properties: { name: 'A' } };
            const term = { urn: 'urn:t:a', type: EntityType.GlossaryTerm, properties: { name: 'B' } };
            const result = getCollapsedGlossaryItems({
                nodes: [node] as unknown as GlossaryNode[],
                terms: [term] as unknown as GlossaryTerm[],
                entityRegistry,
                generateColor,
            });
            expect(result[0].Icon).toBe(getGlossaryEntityIcon(EntityType.GlossaryNode));
            expect(result[1].Icon).toBe(getGlossaryEntityIcon(EntityType.GlossaryTerm));
        });
    });

    describe('deriveGlossaryLabelFromUrn', () => {
        it('returns the leaf segment after the last dot for nested terms', () => {
            // Glossary URNs encode hierarchy after the type prefix; only the leaf is the user-
            // facing name (the rest comes from the entity's parentNodes when hydrated).
            expect(deriveGlossaryLabelFromUrn('urn:li:glossaryTerm:Adoption.HighRisk')).toBe('HighRisk');
            expect(deriveGlossaryLabelFromUrn('urn:li:glossaryNode:Classification.Personal.Identifiable')).toBe(
                'Identifiable',
            );
        });

        it('returns the full id when the URN has no dot-encoded hierarchy', () => {
            expect(deriveGlossaryLabelFromUrn('urn:li:glossaryTerm:Confidential')).toBe('Confidential');
        });

        it('falls back to the full input when it is not a colon-prefixed URN', () => {
            // Defensive: in practice callers always pass a real URN, but the helper should never
            // throw on a malformed string — it's a last-resort label fallback.
            expect(deriveGlossaryLabelFromUrn('Confidential')).toBe('Confidential');
        });
    });
});
