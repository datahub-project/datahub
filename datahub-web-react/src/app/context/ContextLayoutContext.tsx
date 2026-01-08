import React, { createContext, useContext, useMemo } from 'react';

interface ContextLayoutValue {
    /**
     * The current width of the context sidebar in pixels.
     * Used to offset fixed-position elements (like the editor toolbar) to center them on the content area.
     */
    sidebarWidth: number;
}

const ContextLayoutContext = createContext<ContextLayoutValue | null>(null);

interface ContextLayoutProviderProps {
    sidebarWidth: number;
    children: React.ReactNode;
}

export function ContextLayoutProvider({ sidebarWidth, children }: ContextLayoutProviderProps) {
    const value = useMemo(() => ({ sidebarWidth }), [sidebarWidth]);
    return <ContextLayoutContext.Provider value={value}>{children}</ContextLayoutContext.Provider>;
}

/**
 * Hook to get the context layout information.
 * Returns null when not within a ContextLayoutProvider (i.e., outside Context Documents).
 */
export function useContextLayout(): ContextLayoutValue | null {
    return useContext(ContextLayoutContext);
}
