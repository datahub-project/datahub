export const SIDEBAR_TRANSITION_MS = 300;
export const SIDEBAR_COLLAPSED_WIDTH = 63;

/** Drag-resize bounds. Below the minimum, collapse the sidebar instead. */
export const SIDEBAR_MIN_WIDTH = 220;
export const SIDEBAR_MAX_WIDTH = 500;

/** Shared across Glossary / Domains / Documents / Metrics. */
export const SIDEBAR_WIDTH_STORAGE_KEY = 'hierarchicalBrowseSidebarWidth';

/** Search "Navigate" sidebar keeps its own width so it can start narrow. */
export const SEARCH_BROWSE_SIDEBAR_WIDTH_STORAGE_KEY = 'searchBrowseSidebarWidth';
export const SEARCH_BROWSE_SIDEBAR_DEFAULT_WIDTH = 260;

/**
 * Leading entity glyph size for tree rows (expanded + collapsed).
 * Domains / Glossary badges, Phosphor doc/metric icons, and home house all use this.
 */
export const TREE_ROW_ENTITY_ICON_SIZE = 20;

/** Inner glyph size for colored badge icons (DomainColoredIcon / GlossaryColoredIcon). */
export const TREE_ROW_ENTITY_ICON_GLYPH_SIZE = 12;

/** Phosphor expand/collapse caret (Notion-style: swaps over the entity icon). */
export const TREE_ROW_CARET_SIZE = 14;
