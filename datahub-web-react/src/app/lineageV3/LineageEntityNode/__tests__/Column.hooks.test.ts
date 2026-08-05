import { buildRelatedColumnFilters } from '@app/lineageV3/LineageEntityNode/Column.hooks';

import { FilterOperator } from '@types';

const SIBLING = 'urn:li:dataset:(urn:li:dataPlatform:dbt,db.schema.customers,PROD)';

function filters(mergedUrns: Set<string>) {
    return buildRelatedColumnFilters(mergedUrns)[0].and ?? [];
}

describe('buildRelatedColumnFilters', () => {
    it('counts only direct relations, excluding columns on dbt nodes', () => {
        const and = filters(new Set());

        expect(and).toHaveLength(2);
        expect(and[0]).toEqual({ field: 'degree', values: ['1'] });
        expect(and[1]).toEqual({
            field: 'parent',
            values: ['urn:li:dataPlatform:dbt'],
            condition: FilterOperator.Contain,
            negated: true,
        });
    });

    it('excludes columns on nodes drawn as part of the same node', () => {
        const and = filters(new Set([SIBLING]));

        expect(and).toHaveLength(3);
        expect(and[2]).toEqual({ field: 'parent', values: [SIBLING], negated: true });
    });
});
