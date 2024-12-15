import React from 'react';
import { useHistory } from 'react-router';
import { Breadcrumb } from 'antd';
import styled from 'styled-components';
import { BreadcrumbItem, BrowseRow } from './ProfileNavBrowsePath';
import { useEntityData } from '../../../EntityContext';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { navigateToSearchUrl } from '../../../../../search/utils/navigateToSearchUrl';
import { BrowsePathV2, EntityType, FabricType, FacetFilterInput } from '../../../../../../types.generated';
import {
    BROWSE_PATH_V2_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    ORIGIN_FILTER_NAME,
    PLATFORM_FILTER_NAME,
} from '../../../../../search/utils/constants';
import useHasMultipleEnvironmentsQuery from './useHasMultipleEnvironmentsQuery';
import { createBrowseV2SearchFilter } from '../../../../../search/filters/utils';
import { LineageSelector } from './LineageSelector';

const StyledBreadcrumb = styled(Breadcrumb)`
    font-size: 16px;
`;

interface Props {
    urn: string;
    type: EntityType;
}

export default function ProfileNavBrowsePathV2({ urn, type }: Props) {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const { entityData } = useEntityData();
    const isBrowsable = entityRegistry.getBrowseEntityTypes().includes(type);
    const hasEnvironment = !!entityData?.origin;

    const hasMultipleEnvironments = useHasMultipleEnvironmentsQuery(type);

    function handlePathClick(filters: FacetFilterInput[]) {
        navigateToSearchUrl({ query: '*', filters, history });
    }

    function generateFiltersForPlatform() {
        const filters: FacetFilterInput[] = [{ field: ENTITY_SUB_TYPE_FILTER_NAME, values: [type] }];
        if (hasMultipleEnvironments && hasEnvironment) {
            filters.push({ field: ORIGIN_FILTER_NAME, values: [entityData?.origin as FabricType] });
        }
        if (entityData?.platform) {
            filters.push({ field: PLATFORM_FILTER_NAME, values: [entityData.platform.urn] });
        }
        return filters;
    }

    function generateFiltersForBrowsePath(path: string[]) {
        const filters = generateFiltersForPlatform();
        const pathValue = createBrowseV2SearchFilter(path);
        filters.push({ field: BROWSE_PATH_V2_FILTER_NAME, values: [pathValue] });
        return filters;
    }

    return (
        <BrowseRow>
            <StyledBreadcrumb separator=">">
                <BreadcrumbItem
                    disabled={!isBrowsable}
                    onClick={() => handlePathClick([{ field: ENTITY_SUB_TYPE_FILTER_NAME, values: [type] }])}
                >
                    {entityRegistry.getCollectionName(type)}
                </BreadcrumbItem>
                {hasMultipleEnvironments && hasEnvironment && (
                    <BreadcrumbItem
                        disabled={!isBrowsable}
                        onClick={() =>
                            handlePathClick([
                                { field: ENTITY_SUB_TYPE_FILTER_NAME, values: [type] },
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
                        onClick={() => handlePathClick(generateFiltersForPlatform())}
                    >
                        {entityRegistry.getDisplayName(EntityType.DataPlatform, entityData.platform)}
                    </BreadcrumbItem>
                )}
                {entityData?.browsePathV2?.path?.map((pathEntry, index) => (
                    <BreadcrumbItem
                        key={pathEntry.name}
                        disabled={!isBrowsable}
                        onClick={() =>
                            handlePathClick(
                                generateFiltersForBrowsePath([
                                    ...(entityData.browsePathV2 as BrowsePathV2).path
                                        .slice(0, index + 1)
                                        .map((e) => e?.name || ''),
                                ]),
                            )
                        }
                        data-testid={`browse-path-${pathEntry?.name}`}
                    >
                        {pathEntry.entity
                            ? entityRegistry.getDisplayName(pathEntry.entity.type, pathEntry.entity)
                            : pathEntry.name}
                    </BreadcrumbItem>
                ))}
            </StyledBreadcrumb>
            <LineageSelector urn={urn} type={type} />
        </BrowseRow>
    );
}
