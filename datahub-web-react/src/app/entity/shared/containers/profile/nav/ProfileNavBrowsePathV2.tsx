import React from 'react';
import { useHistory } from 'react-router';
import { Breadcrumb } from 'antd';
import { BreadcrumbItem, BrowseRow } from './ProfileNavBrowsePath';
import { useEntityData } from '../../../EntityContext';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { navigateToSearchUrl } from '../../../../../search/utils/navigateToSearchUrl';
import { BrowsePathV2, EntityType, FabricType, FacetFilterInput } from '../../../../../../types.generated';
import {
    BROWSE_PATH_V2_FILTER_NAME,
    ENTITY_FILTER_NAME,
    ORIGIN_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    UNIT_SEPARATOR,
} from '../../../../../search/utils/constants';
import { useAggregateAcrossEntitiesQuery } from '../../../../../../graphql/search.generated';

export default function ProfileNavBrowsePathV2() {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const { entityData, entityType } = useEntityData();
    const isBrowsable = entityRegistry.getBrowseEntityTypes().includes(entityType);
    const hasEnvironment = !!entityData?.origin;

    const { data } = useAggregateAcrossEntitiesQuery({
        variables: { input: { facets: [ORIGIN_FILTER_NAME], query: '*' } },
    });
    const environmentAggs = data?.aggregateAcrossEntities?.facets?.find((facet) => facet.field === ORIGIN_FILTER_NAME);
    const hasMultipleEnvironments = environmentAggs && environmentAggs.aggregations.length > 1;

    function handlePathClick(filters: FacetFilterInput[]) {
        navigateToSearchUrl({ query: '*', filters, history });
    }

    function generateFiltersForEnvironment() {
        const filters: FacetFilterInput[] = [{ field: ENTITY_FILTER_NAME, values: [entityType] }];
        if (hasMultipleEnvironments && hasEnvironment) {
            filters.push({ field: ORIGIN_FILTER_NAME, values: [entityData?.origin as FabricType] });
        }
        if (entityData?.platform) {
            filters.push({ field: PLATFORM_FILTER_NAME, values: [entityData.platform.urn] });
        }
        return filters;
    }

    function generateFiltersForBrowsePath(path: string[]) {
        const filters = generateFiltersForEnvironment();
        const pathEntries = UNIT_SEPARATOR + path.join(UNIT_SEPARATOR);
        filters.push({ field: BROWSE_PATH_V2_FILTER_NAME, values: [pathEntries] });
        return filters;
    }

    return (
        <BrowseRow>
            <Breadcrumb style={{ fontSize: '16px' }} separator=">">
                <BreadcrumbItem
                    disabled={!isBrowsable}
                    onClick={() => handlePathClick([{ field: ENTITY_FILTER_NAME, values: [entityType] }])}
                >
                    {entityRegistry.getCollectionName(entityType)}
                </BreadcrumbItem>
                {hasMultipleEnvironments && hasEnvironment && (
                    <BreadcrumbItem
                        disabled={!isBrowsable}
                        onClick={() =>
                            handlePathClick([
                                { field: ENTITY_FILTER_NAME, values: [entityType] },
                                { field: ORIGIN_FILTER_NAME, values: [entityData?.origin as FabricType] },
                            ])
                        }
                    >
                        {entityData?.origin}
                    </BreadcrumbItem>
                )}
                {entityData?.platform && (
                    <BreadcrumbItem
                        disabled={!isBrowsable}
                        onClick={() => handlePathClick(generateFiltersForEnvironment())}
                    >
                        {entityRegistry.getDisplayName(EntityType.DataPlatform, entityData.platform)}
                    </BreadcrumbItem>
                )}
                {entityData?.browsePathV2?.path.map((pathEntry, index) => (
                    <BreadcrumbItem
                        key={pathEntry?.name}
                        disabled={!isBrowsable}
                        onClick={() =>
                            handlePathClick(
                                generateFiltersForBrowsePath([
                                    ...((entityData.browsePathV2 as BrowsePathV2).path
                                        .slice(0, index + 1)
                                        .map((e) => e?.name) as string[]),
                                ]),
                            )
                        }
                    >
                        {pathEntry?.entity
                            ? entityRegistry.getDisplayName(pathEntry.entity.type, pathEntry.entity)
                            : pathEntry?.name}
                    </BreadcrumbItem>
                ))}
            </Breadcrumb>
        </BrowseRow>
    );
}
