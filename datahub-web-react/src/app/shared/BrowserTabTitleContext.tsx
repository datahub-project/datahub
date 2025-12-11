/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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

export const useBrowserTitle = () => {
    const context = useContext(BrowserTitleContext);
    if (!context) {
        throw new Error('useBrowserTitle must be used within a BrowserTitleProvider');
    }
    return context;
};
