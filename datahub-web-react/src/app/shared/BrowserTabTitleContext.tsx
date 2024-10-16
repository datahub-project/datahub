import React, { createContext, ReactNode, useContext } from 'react';

interface BrowserTitleContextProps {
    title: string;
    updateTitle: (newTitle: string) => void;
}

const BrowserTitleContext = createContext<BrowserTitleContextProps | undefined>(undefined);

export const BrowserTitleProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
    const [title, setTitle] = React.useState<string>('');

    const updateTitle = (newTitle: string) => {
        setTitle(newTitle);
    };

    return <BrowserTitleContext.Provider value={{ title, updateTitle }}>{children}</BrowserTitleContext.Provider>;
};

export const useBrowserTitle = () => {
    const context = useContext(BrowserTitleContext);
    if (!context) {
        throw new Error('useBrowserTitle must be used within a BrowserTitleProvider');
    }
    return context;
};
