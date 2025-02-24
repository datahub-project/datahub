import React from 'react';
import styled from 'styled-components';
import {
    AggregationMetadata,
    EntityType,
    FacetMetadata,
    SearchAcrossLineageResults,
} from '../../../../../../../types.generated';
import { EntityRegistry } from '../../../../../../../entityRegistryContext';
import { pluralize } from '../../../../../../shared/textUtil';

const UNIT_SEPARATOR = '␞';

const SummaryText = styled.span`
    font-weight: bold;
`;

export type LineageDirectionTypeSummary = {
    type: string;
    count: number;
    isEntityType: boolean; // If false, this represents a sub-type.
};

export type LineageDirectionSummary = {
    total: number;
    types: LineageDirectionTypeSummary[];
};

const mergeEntityTypeSubTypeAggregations = (
    entityTypeTotals: Map<EntityType, any>,
    subTypeAggregations?: FacetMetadata,
) => {
    const results = new Map();
    Array.from(entityTypeTotals.keys()).forEach((key) => {
        results.set(key, { count: entityTypeTotals.get(key).count, isEntityType: true });
    });

    // Iterate through sub-type aggregations, add them, and subtract the total from the corresponding entity type.
    subTypeAggregations?.aggregations?.forEach((agg) => {
        const { value } = agg;
        const values = value.split(UNIT_SEPARATOR);
        const entityType = values[0];
        const subType = values.length > 1 ? value.split(UNIT_SEPARATOR)[1] : undefined;
        if (subType) {
            // TODO: Determine what null subtype means.
            results.set(subType, { count: agg.count, isEntityType: false });
            if (results.has(entityType)) {
                const entityTypeValue = results.get(entityType);
                const newCount = entityTypeValue.count - agg.count;
                results.set(entityType, {
                    ...entityTypeValue,
                    count: newCount,
                });
            }
        }
    });
    return results;
};

const getLineageCountsByEntityType = (aggregations: AggregationMetadata[]): Map<EntityType, any> => {
    const result = new Map();
    aggregations.forEach((agg) => {
        result.set(agg.value, { count: agg.count, isEntityType: true });
    });
    return result;
};

const getLineageCountsByType = (results: SearchAcrossLineageResults) => {
    const entityTypeAggregations = results.facets?.find(
        (facet) => facet.field === '_entityType' || facet.field === 'entity',
    );
    const entityTypeTotals =
        (entityTypeAggregations && getLineageCountsByEntityType(entityTypeAggregations.aggregations)) || new Map();
    const subTypeAggregations = results.facets?.find((facet) => facet.field === '_entityType␞typeNames');
    const finalCounts = mergeEntityTypeSubTypeAggregations(entityTypeTotals, subTypeAggregations) || entityTypeTotals;
    const result = Array.from(finalCounts.keys())
        .map((key) => ({
            type: key,
            count: finalCounts.get(key).count,
            isEntityType: finalCounts.get(key).isEntityType,
        }))
        .filter((entry) => entry.count > 0);
    return result;
};

export const getDirectUpstreamSummary = (results: SearchAcrossLineageResults): LineageDirectionSummary => {
    const { total } = results;
    const types = getLineageCountsByType(results);
    return {
        total,
        types,
    };
};

export const getDirectDownstreamSummary = (results: SearchAcrossLineageResults): LineageDirectionSummary => {
    const { total } = results;
    const types = getLineageCountsByType(results);
    return {
        total,
        types,
    };
};

export const getRelatedEntitySummary = (
    summary: LineageDirectionSummary,
    entityRegistry: EntityRegistry,
): React.ReactNode => {
    return (
        <>
            {summary.types.map((type, idx) => {
                return (
                    <SummaryText>
                        {type.count}{' '}
                        {pluralize(
                            type.count,
                            type.isEntityType ? entityRegistry.getEntityName(type.type as EntityType) ?? '' : type.type,
                        ).toLocaleLowerCase()}
                        {idx < summary.types.length - 1 && <>, </>}
                    </SummaryText>
                );
            })}
        </>
    );
};
