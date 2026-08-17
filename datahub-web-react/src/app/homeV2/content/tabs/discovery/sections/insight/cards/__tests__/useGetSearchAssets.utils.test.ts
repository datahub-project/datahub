import { describe, expect, it } from 'vitest';

import { collapseSiblingEntities } from '@app/homeV2/content/tabs/discovery/sections/insight/cards/useGetSearchAssets.utils';

import { Entity, EntityType } from '@types';

function dataset(urn: string, siblings?: { isPrimary: boolean; siblingUrns: string[] }): Entity {
    return {
        urn,
        type: EntityType.Dataset,
        ...(siblings
            ? {
                  siblings: {
                      isPrimary: siblings.isPrimary,
                      siblings: siblings.siblingUrns.map((siblingUrn) => ({ urn: siblingUrn })),
                  },
              }
            : {}),
    } as unknown as Entity;
}

const DBT_URN = 'urn:li:dataset:(urn:li:dataPlatform:dbt,my_db.my_schema.events,PROD)';
const WAREHOUSE_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)';

describe('collapseSiblingEntities', () => {
    it('collapses a sibling pair to the primary sibling', () => {
        const secondary = dataset(DBT_URN, { isPrimary: false, siblingUrns: [WAREHOUSE_URN] });
        const primary = dataset(WAREHOUSE_URN, { isPrimary: true, siblingUrns: [DBT_URN] });

        expect(collapseSiblingEntities([secondary, primary])).toEqual([primary]);
        expect(collapseSiblingEntities([primary, secondary])).toEqual([primary]);
    });

    it('keeps the highest-ranked member when no sibling is primary', () => {
        const first = dataset(DBT_URN, { isPrimary: false, siblingUrns: [WAREHOUSE_URN] });
        const second = dataset(WAREHOUSE_URN, { isPrimary: false, siblingUrns: [DBT_URN] });

        expect(collapseSiblingEntities([first, second])).toEqual([first]);
    });

    it('keeps unrelated entities and preserves their order', () => {
        const a = dataset('urn:li:dataset:a');
        const b = dataset('urn:li:dataset:b');
        const sibling = dataset(DBT_URN, { isPrimary: false, siblingUrns: [WAREHOUSE_URN] });
        const primary = dataset(WAREHOUSE_URN, { isPrimary: true, siblingUrns: [DBT_URN] });

        expect(collapseSiblingEntities([a, sibling, b, primary])).toEqual([a, primary, b]);
    });

    it('collapses a cohort of more than two siblings', () => {
        const third = 'urn:li:dataset:(urn:li:dataPlatform:looker,my_db.my_schema.events,PROD)';
        const entities = [
            dataset(DBT_URN, { isPrimary: false, siblingUrns: [WAREHOUSE_URN] }),
            dataset(third, { isPrimary: false, siblingUrns: [WAREHOUSE_URN] }),
            dataset(WAREHOUSE_URN, { isPrimary: true, siblingUrns: [DBT_URN, third] }),
        ];

        expect(collapseSiblingEntities(entities).map((entity) => entity.urn)).toEqual([WAREHOUSE_URN]);
    });
});
