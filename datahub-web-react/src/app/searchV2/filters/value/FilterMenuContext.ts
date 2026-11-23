import { createContext, useCallback, useContext, useRef } from 'react';

type Commit = () => void;

interface FilterMenuContextValue {
    /** Registers a submenu's commit, run when the parent menu closes. Returns the unregister function. */
    registerCommit: (commit: Commit) => () => void;
    /**
     * Commits every submenu's staged values, then closes the parent menu. `applyNow` applies an
     * immediate choice (a time bucket, a date range) in the same batch.
     */
    commitAndClose: (applyNow?: () => void) => void;
}

/**
 * Provided by menus whose filter submenus stage values until the menu closes (Add filter, More).
 *
 * Submenus can't apply when they hide: antd hides a submenu whenever the pointer moves to another
 * item. Value menus outside a provider (the pill popovers) apply when they hide instead.
 */
const FilterMenuContext = createContext<FilterMenuContextValue | null>(null);

export const FilterMenuProvider = FilterMenuContext.Provider;

export function useFilterMenu() {
    return useContext(FilterMenuContext);
}

/** The parent menu's side: collects its submenus' commits so it can run them all on close. */
export function useFilterMenuCommits() {
    const commitsRef = useRef(new Set<Commit>());

    const registerCommit = useCallback((commit: Commit) => {
        commitsRef.current.add(commit);
        return () => {
            commitsRef.current.delete(commit);
        };
    }, []);

    const commitAll = useCallback(() => {
        commitsRef.current.forEach((commit) => commit());
    }, []);

    return { registerCommit, commitAll };
}
