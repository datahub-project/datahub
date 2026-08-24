import { LineageEntity, NodeContext, getSiblingUrns } from '@app/lineageV3/common';

import { EntityType } from '@types';

const TABLE = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,db.addresses,PROD)';
const SIBLING = 'urn:li:dataset:(urn:li:dataPlatform:dbt,db.addresses,PROD)';

/**
 * `siblings` and `siblingsSearch` are populated in different circumstances -- notably whether
 * `hideDbtSourceInLineage` merged the pair -- so both shapes have to be understood.
 */
function node(urn: string, properties?: Record<string, unknown>): LineageEntity {
    return {
        id: urn,
        urn,
        type: EntityType.Dataset,
        entity: { urn, type: EntityType.Dataset, name: urn, genericEntityProperties: properties } as any,
        isExpanded: {} as any,
        fetchStatus: {} as any,
        filters: {} as any,
    };
}

function nodes(...entries: LineageEntity[]): NodeContext['nodes'] {
    return new Map(entries.map((entry) => [entry.urn, entry]));
}

describe('getSiblingUrns', () => {
    it('reads siblings of a combined entity, given as a sibling search', () => {
        const graph = nodes(node(TABLE, { siblingsSearch: { searchResults: [{ entity: { urn: SIBLING } }] } }));

        expect(getSiblingUrns(TABLE, graph)).toEqual([SIBLING]);
    });

    it('reads siblings of a separated entity, given as sibling properties', () => {
        const graph = nodes(node(TABLE, { siblings: { isPrimary: true, siblings: [{ urn: SIBLING }] } }));

        expect(getSiblingUrns(TABLE, graph)).toEqual([SIBLING]);
    });

    it('returns nothing for an entity without siblings, or one not on the graph', () => {
        expect(getSiblingUrns(TABLE, nodes(node(TABLE)))).toEqual([]);
        expect(getSiblingUrns(TABLE, nodes())).toEqual([]);
    });
});
