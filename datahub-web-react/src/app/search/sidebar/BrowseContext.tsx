import React, { ReactNode, createContext, useContext } from 'react';
import { AggregationMetadata, BrowseResultGroupV2, EntityType, FilterOperator } from '../../../types.generated';
import { createBrowseV2SearchFilter } from '../filters/utils';
import {
    BROWSE_PATH_V2_FILTER_NAME,
    ENTITY_FILTER_NAME,
    ORIGIN_FILTER_NAME,
    PLATFORM_FILTER_NAME,
} from '../utils/constants';
import {
    useBrowseFilterValue,
    useEntityFilterValue,
    useEnvironmentFilterValue,
    useOnChangeFilters,
    usePlatformFilterValue,
    useSelectedFilters,
} from './SidebarContext';

type BrowseContextValue = {
    entityAggregation?: AggregationMetadata;
    environmentAggregation?: AggregationMetadata;
    platformAggregation?: AggregationMetadata;
    browseResultGroup?: BrowseResultGroupV2;
    path: Array<string>;
    isEntitySelected: boolean;
    isEnvironmentSelected: boolean;
    isPlatformSelected: boolean;
    isBrowsePathPrefix: boolean;
    isBrowsePathSelected: boolean;
    onSelect: () => void;
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

const EXCLUDED_FILTER_NAMES = [
    ENTITY_FILTER_NAME,
    ORIGIN_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    BROWSE_PATH_V2_FILTER_NAME,
];

export const BrowseProvider = ({
    children,
    entityAggregation,
    environmentAggregation,
    platformAggregation,
    browseResultGroup,
    parentPath,
}: Props) => {
    // todo - this context is getting busy, let's move some stuff out to the edges
    const selectedFilters = useSelectedFilters();
    const onChangeFilters = useOnChangeFilters();
    const selectedEntity = useEntityFilterValue();
    const selectedEnvironment = useEnvironmentFilterValue();
    const selectedPlatform = usePlatformFilterValue();
    const selectedBrowsePath = useBrowseFilterValue();

    const basePath = parentPath ? [...parentPath] : [];
    const path = browseResultGroup ? [...basePath, browseResultGroup.name] : basePath;
    const browseSearchFilter = createBrowseV2SearchFilter(path);

    // todo - why does second level collapse when we click on it?
    // oh, probably because we're setting search filters
    // maybe need to try to re-open things if filters changed?

    // todo - maybe move these into the isSelected hook since that'll enforce the non-null types of things
    // todo - what if they're both undefined
    const isEntitySelected = !!selectedEntity && selectedEntity === entityAggregation?.value;
    const isEnvironmentSelected =
        isEntitySelected && (!environmentAggregation || selectedEnvironment === environmentAggregation.value);
    const isPlatformSelected =
        isEntitySelected &&
        isEnvironmentSelected &&
        !!selectedPlatform &&
        selectedPlatform === platformAggregation?.value;
    const isBrowsePathPrefix =
        isEntitySelected &&
        isEnvironmentSelected &&
        isPlatformSelected &&
        !!selectedBrowsePath &&
        selectedBrowsePath.startsWith(browseSearchFilter);
    const isBrowsePathSelected = isBrowsePathPrefix && selectedBrowsePath === browseSearchFilter;

    const onSelect = () => {
        const filters = selectedFilters.filter((sf) => !EXCLUDED_FILTER_NAMES.includes(sf.field));

        if (entityAggregation)
            filters.push({
                field: ENTITY_FILTER_NAME,
                condition: FilterOperator.Equal,
                values: [entityAggregation.value],
            });

        if (environmentAggregation)
            filters.push({
                field: ORIGIN_FILTER_NAME,
                condition: FilterOperator.Equal,
                values: [environmentAggregation.value],
            });

        if (platformAggregation) {
            filters.push({
                field: PLATFORM_FILTER_NAME,
                condition: FilterOperator.Equal,
                values: [platformAggregation.value],
            });
        }

        filters.push({
            field: BROWSE_PATH_V2_FILTER_NAME,
            condition: FilterOperator.Equal,
            values: [browseSearchFilter],
        });

        onChangeFilters(filters);
    };

    return (
        <BrowseContext.Provider
            value={{
                entityAggregation,
                environmentAggregation,
                platformAggregation,
                browseResultGroup,
                path,
                isEntitySelected,
                isEnvironmentSelected,
                isPlatformSelected,
                isBrowsePathPrefix,
                isBrowsePathSelected,
                onSelect,
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

export const useIsEntitySelected = () => {
    return useBrowseContext().isEntitySelected;
};

export const useIsEnvironmentSelected = () => {
    return useBrowseContext().isEnvironmentSelected;
};

export const useIsPlatformSelected = () => {
    return useBrowseContext().isPlatformSelected;
};

export const useIsBrowsePathPrefix = () => {
    return useBrowseContext().isBrowsePathPrefix;
};

export const useIsBrowsePathSelected = () => {
    return useBrowseContext().isBrowsePathSelected;
};

export const useOnSelect = () => {
    return useBrowseContext().onSelect;
};
