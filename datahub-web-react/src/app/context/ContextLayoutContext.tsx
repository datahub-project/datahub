import React, { createContext, useContext, useMemo } from 'react';

interface ContextLayoutValue {
    /**
     * The current width of the context sidebar in pixels.
     * Used to offset fixed-position elements (like the editor toolbar) to center them on the content area.
     */
    sidebarWidth: number;
    /**
     * Whether the sidebar should be hidden entirely (for external documents).
     */
    isSidebarHidden: boolean;
    /**
     * Function to control sidebar visibility.
     * Called by DocumentProfile to hide sidebar for external documents.
     */
    setSidebarHidden: (hidden: boolean) => void;
}

const ContextLayoutContext = createContext<ContextLayoutValue | null>(null);

interface ContextLayoutProviderProps {
    sidebarWidth: number;
    isSidebarHidden: boolean;
    setSidebarHidden: (hidden: boolean) => void;
    children: React.ReactNode;
}

export function ContextLayoutProvider({
    sidebarWidth,
    isSidebarHidden,
    setSidebarHidden,
    children,
}: ContextLayoutProviderProps) {
    const value = useMemo(
        () => ({ sidebarWidth, isSidebarHidden, setSidebarHidden }),
        [sidebarWidth, isSidebarHidden, setSidebarHidden],
    );
    return <ContextLayoutContext.Provider value={value}>{children}</ContextLayoutContext.Provider>;
}

/**
 * Hook to get the context layout information.
 * Returns null when not within a ContextLayoutProvider (i.e., outside Context Documents).
 */
export function useContextLayout(): ContextLayoutValue | null {
    return useContext(ContextLayoutContext);
}
