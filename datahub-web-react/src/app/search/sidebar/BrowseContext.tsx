import React, { ReactNode, createContext, useContext, useEffect, useMemo, useState } from 'react';
import { AggregationMetadata, BrowseResultGroupV2, EntityType } from '../../../types.generated';
import useSidebarFilters from './useSidebarFilters';

type BrowseContextValue = {
    entityAggregation: AggregationMetadata;
    environmentAggregation: AggregationMetadata | null;
    platformAggregation: AggregationMetadata | null;
    browseResultGroup: BrowseResultGroupV2 | null;
    path: Array<string> | null;
    filters: ReturnType<typeof useSidebarFilters>;
    filterVersion: number;
};

const BrowseContext = createContext<BrowseContextValue | null>(null);

type Props = {
    children: ReactNode;
    entityAggregation: AggregationMetadata;
    environmentAggregation: AggregationMetadata | null;
    platformAggregation: AggregationMetadata | null;
    browseResultGroup: BrowseResultGroupV2 | null;
    path: Array<string> | null;
};

export const BrowseProvider = ({
    children,
    entityAggregation,
    environmentAggregation,
    platformAggregation,
    browseResultGroup,
    path,
}: Props) => {
    const [filterVersion, setFilterVersion] = useState(0);
    const latestSidebarFilters = useSidebarFilters({
        environment: environmentAggregation?.value,
        platform: platformAggregation?.value,
    });
    const [cachedFilters, setCachedFilters] = useState(latestSidebarFilters);

    useEffect(() => {
        if (latestSidebarFilters !== cachedFilters) {
            setFilterVersion(filterVersion + 1);
            setCachedFilters(latestSidebarFilters);
        }
    }, [cachedFilters, filterVersion, latestSidebarFilters]);

    const value = useMemo(
        () => ({
            entityAggregation,
            environmentAggregation,
            platformAggregation,
            browseResultGroup,
            path,
            filters: cachedFilters,
            filterVersion,
        }),
        [
            browseResultGroup,
            cachedFilters,
            entityAggregation,
            environmentAggregation,
            filterVersion,
            path,
            platformAggregation,
        ],
    );

    return <BrowseContext.Provider value={value}>{children}</BrowseContext.Provider>;
};

const useBrowseContext = (name: string) => {
    const context = useContext(BrowseContext);
    if (context === null)
        throw new Error(`${name || useBrowseContext.name} must be used under a ${BrowseProvider.name}`);
    return context;
};

export const useEntityAggregation = () => {
    return useBrowseContext(useEntityAggregation.name).entityAggregation;
};

export const useEntityType = () => {
    return useEntityAggregation().value as EntityType;
};

export const useEnvironmentAggregation = () => {
    return useBrowseContext(useEnvironmentAggregation.name).environmentAggregation;
};

export const usePlatformAggregation = () => {
    const context = useBrowseContext(usePlatformAggregation.name);
    if (!context.platformAggregation) throw new Error('platformAggregation is missing in context');
    return context.platformAggregation;
};

export const useBrowseResultGroup = () => {
    const context = useBrowseContext(useBrowseResultGroup.name);
    if (!context.browseResultGroup) throw new Error('browseResultGroup is missing in context');
    return context.browseResultGroup;
};

export const useBrowsePath = () => {
    const context = useBrowseContext(useBrowsePath.name);
    if (!context.path) throw new Error('path is missing in context');
    return context.path;
};

export const useFilters = () => {
    return useBrowseContext(useFilters.name).filters;
};

export const useFilterVersion = () => {
    return useBrowseContext(useFilterVersion.name).filterVersion;
};
