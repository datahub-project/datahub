import React, { ReactNode, createContext, useContext, useMemo } from 'react';
import { AggregationMetadata, BrowseResultGroupV2, EntityType } from '../../../types.generated';

type BrowseContextValue = {
    entityAggregation: AggregationMetadata;
    environmentAggregation?: AggregationMetadata | null;
    platformAggregation?: AggregationMetadata | null;
    browseResultGroup?: BrowseResultGroupV2 | null;
    path: Array<string>;
};

const BrowseContext = createContext<BrowseContextValue | null>(null);

type Props = {
    children: ReactNode;
    entityAggregation: AggregationMetadata;
    environmentAggregation?: AggregationMetadata | null;
    platformAggregation?: AggregationMetadata | null;
    browseResultGroup?: BrowseResultGroupV2 | null;
    parentPath?: Array<string> | null;
};

export const BrowseProvider = ({
    children,
    entityAggregation,
    environmentAggregation,
    platformAggregation,
    browseResultGroup,
    parentPath,
}: Props) => {
    const path = useMemo(() => {
        const basePath = [...(parentPath ?? [])];
        return browseResultGroup ? [...basePath, browseResultGroup.name] : basePath;
    }, [browseResultGroup, parentPath]);

    const value = useMemo(
        () => ({
            entityAggregation,
            environmentAggregation,
            platformAggregation,
            browseResultGroup,
            path,
        }),
        [browseResultGroup, entityAggregation, environmentAggregation, path, platformAggregation],
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

export const useMaybeEnvironmentAggregation = () => {
    return useBrowseContext(useMaybeEnvironmentAggregation.name).environmentAggregation;
};

export const useMaybePlatformAggregation = () => {
    return useBrowseContext(useMaybePlatformAggregation.name).platformAggregation;
};

export const usePlatformAggregation = () => {
    const platformAggregation = useMaybePlatformAggregation();
    if (!platformAggregation) throw new Error('platformAggregation is missing in context');
    return platformAggregation;
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
