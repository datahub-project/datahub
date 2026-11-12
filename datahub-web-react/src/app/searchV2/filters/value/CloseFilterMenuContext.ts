import { createContext, useContext } from 'react';

/**
 * Closes the menu a value menu is rendered in, for explicit commits (Enter, an immediate choice).
 *
 * The More menu applies staged values when a submenu hides, which also happens while the pointer
 * moves between items, so applying must not close it.
 * Popover filters close on any change and don't need to provide this.
 */
const CloseFilterMenuContext = createContext<() => void>(() => {});

export const CloseFilterMenuProvider = CloseFilterMenuContext.Provider;

export function useCloseFilterMenu() {
    return useContext(CloseFilterMenuContext);
}
