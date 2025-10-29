import React, { ReactNode, createContext, useContext } from 'react';

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
