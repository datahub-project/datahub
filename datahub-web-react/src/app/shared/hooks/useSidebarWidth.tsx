import React, { ReactNode, createContext, useContext, useMemo } from 'react';

interface SidebarWidthContextType {
    sidebarWidth: string;
}

const SidebarWidthContext = createContext<SidebarWidthContextType | undefined>(undefined);

interface SidebarWidthProviderProps {
    children: ReactNode;
    isCollapsed: boolean;
}

export const SidebarWidthProvider: React.FC<SidebarWidthProviderProps> = ({ children, isCollapsed }) => {
    const sidebarWidth = useMemo(() => (isCollapsed ? '60px' : '264px'), [isCollapsed]);

    const value = useMemo(() => ({ sidebarWidth }), [sidebarWidth]);

    return <SidebarWidthContext.Provider value={value}>{children}</SidebarWidthContext.Provider>;
};

export const useSidebarWidth = (): SidebarWidthContextType => {
    const context = useContext(SidebarWidthContext);
    if (!context) {
        throw new Error('useSidebarWidth must be used within a SidebarWidthProvider');
    }
    return context;
};
