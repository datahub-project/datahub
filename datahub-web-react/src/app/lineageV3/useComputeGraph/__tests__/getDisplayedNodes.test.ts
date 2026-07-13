import {
    FetchStatus,
    LINEAGE_FILTER_TYPE,
    LineageEntity,
    NodeContext,
    createLineageFilterNodeId,
} from '@app/lineageV3/common';
import getDisplayedNodes from '@app/lineageV3/useComputeGraph/getDisplayedNodes';

import { EntityType, LineageDirection } from '@types';

const ROOT = 'root';
// More than LINEAGE_FILTER_PAGINATION (4), so a lineage filter is created for the root's children
const CHILDREN = ['c0', 'c1', 'c2', 'c3', 'c4', 'c5'];
const LIMIT = 2;

function createNode(urn: string, limit?: number): LineageEntity {
    return {
        id: urn,
        urn,
        type: EntityType.Dataset,
        isExpanded: { [LineageDirection.Upstream]: true, [LineageDirection.Downstream]: true },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.COMPLETE,
            [LineageDirection.Downstream]: FetchStatus.COMPLETE,
        },
        filters: {
            [LineageDirection.Upstream]: { facetFilters: new Map() },
            [LineageDirection.Downstream]: { facetFilters: new Map(), limit },
        },
    };
}

function buildContext(): Pick<NodeContext, 'adjacencyList' | 'nodes' | 'edges' | 'rootType'> {
    const nodes = new Map<string, LineageEntity>([[ROOT, createNode(ROOT, LIMIT)]]);
    CHILDREN.forEach((urn) => nodes.set(urn, createNode(urn)));
    return {
        nodes,
        edges: new Map(),
        rootType: EntityType.Dataset,
        adjacencyList: {
            [LineageDirection.Downstream]: new Map([[ROOT, new Set(CHILDREN)]]),
            [LineageDirection.Upstream]: new Map(CHILDREN.map((urn) => [urn, new Set([ROOT])])),
        },
    };
}

// Children in priority order; pagination should keep the first `limit`
const orderedNodes = () => {
    const context = buildContext();
    return {
        [LineageDirection.Downstream]: CHILDREN.map((urn) => context.nodes.get(urn) as LineageEntity),
        [LineageDirection.Upstream]: [],
    };
};

describe('getDisplayedNodes pagination and filter nodes', () => {
    const filterId = createLineageFilterNodeId(ROOT, LineageDirection.Downstream);

    it('keeps the first `limit` children and records full pagination state', () => {
        const { displayedNodes, lineageFilters } = getDisplayedNodes(ROOT, orderedNodes(), buildContext());

        const shownChildren = displayedNodes.filter((n) => CHILDREN.includes(n.id)).map((n) => n.id);
        expect(shownChildren).toEqual(['c0', 'c1']);

        const filter = lineageFilters.get(filterId);
        expect(filter).toBeDefined();
        expect(filter?.contents).toEqual(CHILDREN);
        expect(filter?.shown).toEqual(new Set(['c0', 'c1']));
    });

    it('renders a filter node by default', () => {
        const { displayedNodes } = getDisplayedNodes(ROOT, orderedNodes(), buildContext());
        expect(displayedNodes.some((n) => n.type === LINEAGE_FILTER_TYPE)).toBe(true);
    });

    it('omits the filter node but still returns its state when createFilterNodes is false', () => {
        const { displayedNodes, lineageFilters } = getDisplayedNodes(ROOT, orderedNodes(), buildContext(), {
            createFilterNodes: false,
        });

        expect(displayedNodes.some((n) => n.type === LINEAGE_FILTER_TYPE)).toBe(false);
        // Children are still shown; only the filter card is suppressed
        expect(displayedNodes.filter((n) => CHILDREN.includes(n.id)).map((n) => n.id)).toEqual(['c0', 'c1']);
        expect(lineageFilters.get(filterId)?.shown).toEqual(new Set(['c0', 'c1']));
    });
});
