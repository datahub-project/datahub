import React, { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { SectionKeys } from './utils';
import useGetTimeseriesCapabilities from './graphs/hooks/useGetTimeseriesCapabilities';

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

interface StatsSectionsContextProps {
    sections: Record<SectionKeys, Section>;
    setSectionState: (key: SectionKeys, hasData: boolean) => void;
    dataInfo: DataInfo;
}

// Function to get default initial sections
const getDefaultSections = (): Record<SectionKeys, Section> => {
    const keys = Object.values(SectionKeys);
    return Object.fromEntries(
        keys.map((key) => [key, { hasData: false, ref: React.createRef<HTMLDivElement>() }]),
    ) as Record<SectionKeys, Section>;
};

const defaultDataInfo = { capabilitiesLoading: false };

const StatsSectionsContext = createContext<StatsSectionsContextProps>({
    sections: getDefaultSections(),
    setSectionState: () => {},
    dataInfo: defaultDataInfo,
});

export const useStatsSectionsContext = () => useContext(StatsSectionsContext);

interface Props {
    children: React.ReactNode;
}

export const StatsSectionsContextProvider = ({ children }: Props) => {
    const { urn } = useEntityData();
    const [sections, setSections] = useState<Record<SectionKeys, Section>>(getDefaultSections);
    const [dataInfo, setDataInfo] = useState<DataInfo>(defaultDataInfo);

    const { data, loading } = useGetTimeseriesCapabilities(urn);

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

    const value = useMemo(() => ({ sections, setSectionState, dataInfo }), [sections, setSectionState, dataInfo]);

    return <StatsSectionsContext.Provider value={value}>{children}</StatsSectionsContext.Provider>;
};
