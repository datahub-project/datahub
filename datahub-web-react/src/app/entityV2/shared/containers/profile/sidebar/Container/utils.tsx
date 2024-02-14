import React from 'react';
import styled from 'styled-components';
import { AggregationMetadata, EntityType, FacetMetadata, SearchResults } from '../../../../../../../types.generated';
import { pluralize } from '../../../../../../shared/textUtil';
import { EntityRegistry } from '../../../../../../../entityRegistryContext';
import { ANTD_GRAY } from '../../../../constants';

const UNIT_SEPARATOR = '␞';

const SummaryText = styled.span`
    color: ${ANTD_GRAY[7]};
`;

export type ContentTypeSummary = {
    type: string;
    count: number;
    isEntityType: boolean; // If false, this represents a sub-type.
};

export type ContentsSummary = {
    total: number;
    types: ContentTypeSummary[];
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
        const subType = values.length > 1 ? values[1] : undefined;

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

const getContentCountsByEntityType = (aggregations: AggregationMetadata[]): Map<EntityType, any> => {
    const result = new Map();
    aggregations.forEach((agg) => {
        result.set(agg.value, { count: agg.count, isEntityType: true });
    });
    return result;
};

const getContentCountsByType = (results: SearchResults) => {
    const entityTypeAggregations = results.facets?.find((facet) => facet.field === '_entityType');
    const entityTypeTotals =
        (entityTypeAggregations && getContentCountsByEntityType(entityTypeAggregations.aggregations)) || new Map();
    const subTypeAggregations = results.facets?.find((facet) => facet.field === '_entityType␞typeNames');
    const finalCounts = mergeEntityTypeSubTypeAggregations(entityTypeTotals, subTypeAggregations) || entityTypeTotals;
    const result = Array.from(finalCounts.keys())
        .map((key) => ({
            type: key,
            count: finalCounts.get(key).count,
            isEntityType: finalCounts.get(key).isEntityType,
        }))
        .filter((entry) => entry.count > 0);
    return {
        types: result,
        total: result.reduce((accumulator, entry) => accumulator + entry.count, 0),
    };
};

export const getContentsSummary = (contents: SearchResults): ContentsSummary => {
    return getContentCountsByType(contents);
};

export const getContentsSummaryText = (summary: ContentsSummary, entityRegistry: EntityRegistry): React.ReactNode => {
    return (
        <>
            {summary.types.map((type, idx) => {
                return (
                    <SummaryText>
                        {type.count}{' '}
                        {pluralize(
                            type.count,
                            type.isEntityType
                                ? (entityRegistry.getEntityName(type.type as EntityType) as any)
                                : type.type,
                        ).toLocaleLowerCase()}
                        {idx < summary.types.length - 1 && <>, </>}
                    </SummaryText>
                );
            })}
        </>
    );
};

export const navigateToContainerContents = (urn, type, history, entityRegistry) => {
    history.push(`${entityRegistry.getEntityUrl(type, urn)}/Contents`);
};
