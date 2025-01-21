import { useBaseEntity } from '@src/app/entity/shared/EntityContext';
import { GenericEntityProperties } from '@src/app/entity/shared/types';
import { GetDatasetQuery } from '@src/graphql/dataset.generated';
import { Entity } from '@src/types.generated';
import React, { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';
import useGetTimeseriesCapabilities from './graphs/hooks/useGetTimeseriesCapabilities';
import { getSiblingEntityWithStats, SectionKeys } from './utils';

export interface Section {
    hasData: boolean;
    ref: React.RefObject<HTMLDivElement>;
}

interface DataInfo {
    capabilitiesLoading: boolean;
    oldestDatasetProfileTime?: number | null;
    oldestDatasetUsageTime?: number | null;
    oldestOperationTime?: number | null;
}

interface StatsPermissions {
    canViewDatasetUsage: boolean;
    canViewDatasetProfile: boolean;
    canViewDatasetOperations: boolean;
}

interface StatsSectionsContextProps {
    sections: Record<SectionKeys, Section>;
    setSectionState: (key: SectionKeys, hasData: boolean) => void;
    dataInfo: DataInfo;
    statsEntity: Entity | undefined;
    statsEntityUrn: string | undefined;
    setStatsEntityUrn: React.Dispatch<React.SetStateAction<string | undefined>>;
    permissions: StatsPermissions;
}

// Function to get default initial sections
const getDefaultSections = (): Record<SectionKeys, Section> => {
    const keys = Object.values(SectionKeys);
    return Object.fromEntries(
        keys.map((key) => [key, { hasData: false, ref: React.createRef<HTMLDivElement>() }]),
    ) as Record<SectionKeys, Section>;
};

const defaultDataInfo = { capabilitiesLoading: false };

const defaultPermissions = {
    canViewDatasetUsage: false,
    canViewDatasetProfile: false,
    canViewDatasetOperations: false,
};

const StatsSectionsContext = createContext<StatsSectionsContextProps>({
    sections: getDefaultSections(),
    setSectionState: () => {},
    dataInfo: defaultDataInfo,
    statsEntity: undefined,
    statsEntityUrn: undefined,
    setStatsEntityUrn: () => {},
    permissions: defaultPermissions,
});

export const useStatsSectionsContext = () => useContext(StatsSectionsContext);

interface Props {
    children: React.ReactNode;
}

export const StatsSectionsContextProvider = ({ children }: Props) => {
    const [sections, setSections] = useState<Record<SectionKeys, Section>>(getDefaultSections);
    const [dataInfo, setDataInfo] = useState<DataInfo>(defaultDataInfo);

    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const [statsEntityUrn, setStatsEntityUrn] = useState(getSiblingEntityWithStats(baseEntity));

    const statsEntity: any =
        baseEntity.dataset?.urn !== statsEntityUrn
            ? baseEntity.dataset?.siblingsSearch?.searchResults.find((res) => res.entity.urn === statsEntityUrn)
                  ?.entity || baseEntity.dataset?.siblingsSearch?.searchResults[0]?.entity
            : baseEntity.dataset;

    const { data, loading } = useGetTimeseriesCapabilities(statsEntityUrn);

    useEffect(() => {
        if (loading) {
            setDataInfo({ capabilitiesLoading: true });
        } else if (data) {
            const { oldestDatasetProfileTime, oldestDatasetUsageTime, oldestOperationTime } = data;
            setDataInfo({
                capabilitiesLoading: false,
                oldestDatasetProfileTime,
                oldestDatasetUsageTime,
                oldestOperationTime,
            });
        }
    }, [data, loading]);

    // Function to update if a section has data or not
    const setSectionState = useCallback((key: SectionKeys, hasData: boolean) => {
        setSections((prev) => ({
            ...prev,
            [key]: { ...prev[key], hasData },
        }));
    }, []);

    const canViewDatasetUsage = (statsEntity as GenericEntityProperties)?.privileges?.canViewDatasetUsage;
    const canViewDatasetProfile = (statsEntity as GenericEntityProperties)?.privileges?.canViewDatasetProfile;
    const canViewDatasetOperations = (statsEntity as GenericEntityProperties)?.privileges?.canViewDatasetOperations;

    const value = useMemo(
        () => ({
            sections,
            setSectionState,
            dataInfo,
            statsEntity,
            statsEntityUrn,
            setStatsEntityUrn,
            permissions: {
                canViewDatasetUsage: !!canViewDatasetUsage,
                canViewDatasetProfile: !!canViewDatasetProfile,
                canViewDatasetOperations: !!canViewDatasetOperations,
            },
        }),
        [
            sections,
            setSectionState,
            dataInfo,
            statsEntity,
            statsEntityUrn,
            setStatsEntityUrn,
            canViewDatasetUsage,
            canViewDatasetProfile,
            canViewDatasetOperations,
        ],
    );

    return <StatsSectionsContext.Provider value={value}>{children}</StatsSectionsContext.Provider>;
};
