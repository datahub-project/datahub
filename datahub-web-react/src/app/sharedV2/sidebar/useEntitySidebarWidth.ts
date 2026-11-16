import usePersistedSidebarWidth from '@app/sharedV2/sidebar/usePersistedSidebarWidth';

/** Shared by every surface that renders the entity profile sidebar, so one drag applies everywhere. */
const ENTITY_SIDEBAR_WIDTH_STORAGE_KEY = 'entityProfileSidebarWidth';
export const ENTITY_SIDEBAR_MIN_WIDTH = 320;
export const ENTITY_SIDEBAR_MAX_WIDTH = 800;
const ENTITY_SIDEBAR_DEFAULT_RATIO = 0.3;

/** Drag-resizable, persisted width for the entity profile sidebar (entity page, search, lineage, ...). */
export default function useEntitySidebarWidth(): { width: number; setWidth: (width: number) => void } {
    return usePersistedSidebarWidth({
        storageKey: ENTITY_SIDEBAR_WIDTH_STORAGE_KEY,
        minWidth: ENTITY_SIDEBAR_MIN_WIDTH,
        maxWidth: ENTITY_SIDEBAR_MAX_WIDTH,
        defaultRatio: ENTITY_SIDEBAR_DEFAULT_RATIO,
    });
}
