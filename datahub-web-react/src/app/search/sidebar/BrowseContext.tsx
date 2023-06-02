import React, { ReactNode, createContext, useContext } from 'react';
import {
    AggregationMetadata,
    BrowseResultGroupV2,
    EntityType,
    FacetFilterInput,
    FilterOperator,
} from '../../../types.generated';
import { createBrowseV2SearchFilter } from '../filters/utils';
import {
    BROWSE_PATH_V2_FILTER_NAME,
    ENTITY_FILTER_NAME,
    ORIGIN_FILTER_NAME,
    PLATFORM_FILTER_NAME,
} from '../utils/constants';
import { useIsMatchingFilter, useOnChangeFilters, useSelectedFilters } from './SidebarContext';
import { applyFacetFilterOverrides } from '../utils/applyFilterOverrides';

type BrowseContextValue = {
    entityAggregation?: AggregationMetadata;
    environmentAggregation?: AggregationMetadata;
    platformAggregation?: AggregationMetadata;
    browseResultGroup?: BrowseResultGroupV2;
    path: Array<string>;
};

const BrowseContext = createContext<BrowseContextValue | null>(null);

type Props = {
    children: ReactNode;
    entityAggregation?: AggregationMetadata;
    environmentAggregation?: AggregationMetadata;
    platformAggregation?: AggregationMetadata;
    browseResultGroup?: BrowseResultGroupV2;
    parentPath?: Array<string>;
};

export const BrowseProvider = ({
    children,
    entityAggregation,
    environmentAggregation,
    platformAggregation,
    browseResultGroup,
    parentPath,
}: Props) => {
    const basePath = parentPath ? [...parentPath] : [];
    const path = browseResultGroup ? [...basePath, browseResultGroup.name] : basePath;

    return (
        <BrowseContext.Provider
            value={{
                entityAggregation,
                environmentAggregation,
                platformAggregation,
                browseResultGroup,
                path,
            }}
        >
            {children}
        </BrowseContext.Provider>
    );
};

const useBrowseContext = () => {
    const context = useContext(BrowseContext);
    if (context === null) throw new Error(`${useBrowseContext.name} must be used under a ${BrowseProvider.name}`);
    return context;
};

export const useMaybeEntityAggregation = () => {
    return useBrowseContext().entityAggregation;
};

export const useMaybeEntityType = () => {
    const entityAggregation = useMaybeEntityAggregation();
    return entityAggregation ? (entityAggregation.value as EntityType) : null;
};

export const useEntityAggregation = () => {
    const entityAggregation = useMaybeEntityAggregation();
    if (!entityAggregation) throw new Error('entityAggregation is missing in context');
    return entityAggregation;
};

export const useEntityType = () => {
    return useEntityAggregation().value as EntityType;
};

export const useMaybeEnvironmentAggregation = () => {
    return useBrowseContext().environmentAggregation;
};

export const useMaybePlatformAggregation = () => {
    return useBrowseContext().platformAggregation;
};

export const usePlatformAggregation = () => {
    const platformAggregation = useMaybePlatformAggregation();
    if (!platformAggregation) throw new Error('platformAggregation is missing in context');
    return platformAggregation;
};

export const useBrowseResultGroup = () => {
    const context = useBrowseContext();
    if (!context.browseResultGroup) throw new Error('browseResultGroup is missing in context');
    return context.browseResultGroup;
};

export const useBrowsePath = () => {
    const context = useBrowseContext();
    if (!context.path) throw new Error('path is missing in context');
    return context.path;
};

export const useBrowsePathLength = () => {
    return useBrowsePath().length;
};

export const useBrowseSearchFilter = () => {
    return createBrowseV2SearchFilter(useBrowsePath());
};

export const useIsEntitySelected = () => {
    return useIsMatchingFilter(ENTITY_FILTER_NAME, useEntityAggregation().value);
};

export const useIsEnvironmentSelected = () => {
    const environmentAggregation = useMaybeEnvironmentAggregation();
    const isEntitySelected = useIsEntitySelected();
    const isEnvironmentSelected = useIsMatchingFilter(ORIGIN_FILTER_NAME, environmentAggregation?.value);
    return isEntitySelected && (!environmentAggregation || isEnvironmentSelected);
};

export const useIsPlatformSelected = () => {
    const isEntitySelected = useIsEntitySelected();
    const isEnvironmentSelected = useIsEnvironmentSelected();
    const isPlatformSelected = useIsMatchingFilter(PLATFORM_FILTER_NAME, usePlatformAggregation().value);
    return isEntitySelected && isEnvironmentSelected && isPlatformSelected;
};

export const useIsBrowsePathPrefix = () => {
    const isEntitySelected = useIsEntitySelected();
    const isEnvironmentSelected = useIsEnvironmentSelected();
    const isPlatformSelected = useIsPlatformSelected();
    const isBrowsePathPrefix = useIsMatchingFilter(BROWSE_PATH_V2_FILTER_NAME, useBrowseSearchFilter(), {
        prefix: true,
    });
    return isEntitySelected && isEnvironmentSelected && isPlatformSelected && isBrowsePathPrefix;
};

export const useIsBrowsePathSelected = () => {
    const isEntitySelected = useIsEntitySelected();
    const isEnvironmentSelected = useIsEnvironmentSelected();
    const isPlatformSelected = useIsPlatformSelected();
    const isBrowsePathSelected = useIsMatchingFilter(BROWSE_PATH_V2_FILTER_NAME, useBrowseSearchFilter());
    return isEntitySelected && isEnvironmentSelected && isPlatformSelected && isBrowsePathSelected;
};

export const useOnSelectBrowsePath = () => {
    const entityAggregation = useEntityAggregation();
    const environmentAggregation = useMaybeEnvironmentAggregation();
    const platformAggregation = usePlatformAggregation();
    const browseSearchFilter = useBrowseSearchFilter();
    const selectedFilters = useSelectedFilters();
    const onChangeFilters = useOnChangeFilters();

    return () => {
        const overrides: Array<FacetFilterInput> = [];

        overrides.push({
            field: ENTITY_FILTER_NAME,
            condition: FilterOperator.Equal,
            values: [entityAggregation.value],
        });

        if (environmentAggregation)
            overrides.push({
                field: ORIGIN_FILTER_NAME,
                condition: FilterOperator.Equal,
                values: [environmentAggregation.value],
            });

        overrides.push({
            field: PLATFORM_FILTER_NAME,
            condition: FilterOperator.Equal,
            values: [platformAggregation.value],
        });

        overrides.push({
            field: BROWSE_PATH_V2_FILTER_NAME,
            condition: FilterOperator.Equal,
            values: [browseSearchFilter],
        });

        const filtersWithOverrides = applyFacetFilterOverrides(selectedFilters, overrides);

        onChangeFilters(filtersWithOverrides);
    };
};
