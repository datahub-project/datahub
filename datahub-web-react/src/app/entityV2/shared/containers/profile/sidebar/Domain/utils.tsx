import React from 'react';
import styled from 'styled-components';
import * as QueryString from 'query-string';
import { AggregationMetadata, EntityType, FacetMetadata, SearchResults } from '../../../../../../../types.generated';
import filtersToQueryStringParams from '../../../../../../search/utils/filtersToQueryStringParams';
import { pluralize } from '../../../../../../shared/textUtil';
import { EntityRegistry } from '../../../../../../../entityRegistryContext';
import { ANTD_GRAY } from '../../../../constants';

const UNIT_SEPARATOR = '␞';

const SummaryText = styled.span`
    color: ${ANTD_GRAY[7]};
`;

export type ContentTypeSummary = {
    entityType: EntityType;
    type?: string;
    count: number;
};

export type ContentsSummary = {
    total: number;
    types: ContentTypeSummary[];
};

const mergeEntityTypeSubTypeAggregations = (
    entityTypeTotals: Map<EntityType, any>,
    subTypeAggregations?: FacetMetadata,
) => {
    const typeToSummary = new Map();
    Array.from(entityTypeTotals.keys()).forEach((key) => {
        typeToSummary.set(key, { count: entityTypeTotals.get(key), entityType: key });
    });

    // Iterate through sub-type aggregations, add them, and subtract the total from the corresponding entity type.
    subTypeAggregations?.aggregations?.forEach((agg) => {
        const { value } = agg;
        const values = value.split(UNIT_SEPARATOR);
        const entityType = values[0];
        const subType = values.length > 1 ? values[1] : undefined;
        if (subType) {
            typeToSummary.set(subType, { count: agg.count, entityType, type: subType });
            if (typeToSummary.has(entityType)) {
                const entityTypeValue = typeToSummary.get(entityType);
                const newCount = entityTypeValue.count - agg.count;
                typeToSummary.set(entityType, {
                    ...entityTypeValue,
                    count: newCount,
                });
            }
        }
    });
    return Array.from(typeToSummary.keys())
        .map((key) => ({
            entityType: typeToSummary.get(key).entityType,
            type: typeToSummary.get(key).type,
            count: typeToSummary.get(key).count,
        }))
        .filter((entry) => entry.count > 0)
        .sort((a, b) => b.count - a.count);
};

const getContentCountsByEntityType = (aggregations: AggregationMetadata[]): Map<EntityType, any> => {
    const result = new Map();
    aggregations.forEach((agg) => {
        result.set(agg.value, agg.count);
    });
    return result;
};

const getContentCountsByType = (results: SearchResults) => {
    const entityTypeAggregations = results.facets?.find((facet) => facet.field === '_entityType');
    const entityTypeTotals =
        (entityTypeAggregations && getContentCountsByEntityType(entityTypeAggregations.aggregations)) || new Map();
    const subTypeAggregations = results.facets?.find((facet) => facet.field === '_entityType␞typeNames');
    const result = mergeEntityTypeSubTypeAggregations(entityTypeTotals, subTypeAggregations);
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
                    <SummaryText key={type.entityType}>
                        {type.count}{' '}
                        {pluralize(
                            type.count,
                            !type.type
                                ? (entityRegistry.getEntityName(type.entityType as EntityType) as any)
                                : type.type,
                        ).toLocaleLowerCase()}
                        {idx < summary.types.length - 1 && <>, </>}
                    </SummaryText>
                );
            })}
        </>
    );
};

export const navigateToDomainEntities = (urn, type, history, entityRegistry) => {
    history.push(`${entityRegistry.getEntityUrl(type, urn)}/Assets`);
};

export function getDomainEntitiesFilterUrl(urn, type, entityRegistry, types, subTypes): string {
    const filters = [
        {
            field: '_entityType',
            values: types,
            negated: false,
        },
    ];

    if (subTypes) {
        filters.push({
            field: 'typeNames',
            values: subTypes,
            negated: false,
        });
    }

    const search = QueryString.stringify(filtersToQueryStringParams(filters), { arrayFormat: 'comma' });

    return `${entityRegistry.getEntityUrl(type, urn)}/Assets?${search}`;
}

export const navigateToDomainDataProducts = (urn, type, history, entityRegistry, isAdd = false) => {
    history.push(`${entityRegistry.getEntityUrl(type, urn)}/Data Products${isAdd ? '?createModal=true' : ''}`);
};
