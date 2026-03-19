import React, { useState } from 'react';

export interface IngestionContextType {
    createdOrUpdatedSource?: string | undefined;
    setCreatedOrUpdatedSource: (urn: string | undefined) => void;
    shouldRunCreatedOrUpdatedSource?: boolean;
    setShouldRunCreatedOrUpdatedSource: (value: boolean) => void;
}

const IngestionContext = React.createContext<IngestionContextType>({
    setCreatedOrUpdatedSource: () => {},
    setShouldRunCreatedOrUpdatedSource: () => {},
});

export function useIngestionContext() {
    return React.useContext<IngestionContextType>(IngestionContext);
}

interface Props {
    children: React.ReactNode;
}

export function IngestionContextProvider({ children }: Props) {
    const [createdOrUpdatedSource, setCreatedOrUpdatedSource] = useState<string | undefined>(undefined);
    const [shouldRunCreatedOrUpdatedSource, setShouldRunCreatedOrUpdatedSource] = useState<boolean>(false);

    return (
        <IngestionContext.Provider
            value={{
                createdOrUpdatedSource,
                setCreatedOrUpdatedSource,
                shouldRunCreatedOrUpdatedSource,
                setShouldRunCreatedOrUpdatedSource,
            }}
        >
            {children}
        </IngestionContext.Provider>
    );
}
