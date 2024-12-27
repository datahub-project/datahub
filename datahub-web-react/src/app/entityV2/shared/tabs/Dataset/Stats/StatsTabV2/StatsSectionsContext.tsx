import React, { createContext, useCallback, useContext, useMemo, useState } from 'react';
import { SectionKeys } from './utils';

export interface Section {
    hasData: boolean;
    ref: React.RefObject<HTMLDivElement>;
}

interface StatsSectionsContextProps {
    sections: Record<SectionKeys, Section>;
    setSectionState: (key: SectionKeys, hasData: boolean) => void;
}

// Function to get default initial sections
const getDefaultSections = (): Record<SectionKeys, Section> => {
    const keys: SectionKeys[] = ['rowsAndUsers', 'queries', 'storage', 'changes', 'columnStats'];
    return Object.fromEntries(
        keys.map((key) => [key, { hasData: false, ref: React.createRef<HTMLDivElement>() }]),
    ) as Record<SectionKeys, Section>;
};

const StatsSectionsContext = createContext<StatsSectionsContextProps>({
    sections: getDefaultSections(),
    setSectionState: () => {},
});

export const useStatsSectionsContext = () => useContext(StatsSectionsContext);

interface Props {
    children: React.ReactNode;
}

export const StatsSectionsContextProvider = ({ children }: Props) => {
    const [sections, setSections] = useState<Record<SectionKeys, Section>>(getDefaultSections);

    // Function to update if a section has data or not
    const setSectionState = useCallback((key: SectionKeys, hasData: boolean) => {
        setSections((prev) => ({
            ...prev,
            [key]: { ...prev[key], hasData },
        }));
    }, []);

    const value = useMemo(() => ({ sections, setSectionState }), [sections, setSectionState]);

    return <StatsSectionsContext.Provider value={value}>{children}</StatsSectionsContext.Provider>;
};
