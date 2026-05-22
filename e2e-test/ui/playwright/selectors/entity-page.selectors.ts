/**
 * Entity page selectors — centralised data-testid strings for entity profile pages.
 *
 * All data-testid values are defined here as `as const` so renames require
 * a single-file change. Factory functions (`tab(name)`) keep dynamic selectors
 * co-located with related static ones.
 */

export const ENTITY_PAGE = {
  // Header
  entityName: '[data-testid="entity-name"]',
  deprecationBadge: '[data-testid="deprecation-badge"]',
  threeDotMenu: '[data-testid="entity-header-dropdown"]',

  // Health badge
  healthIcon: (urn: string) => `[data-testid="${urn}-health-icon"]`,
  assertionsDetails: '[data-testid="assertions-details"]',

  // Tab headers — factory function for arbitrary tab names
  tabHeader: (name: string) => `[data-testid="${name}-entity-tab-header"]`,

  // Ownership types settings
  createOwnerTypeButton: '[data-testid="create-owner-type-v2"]',
  ownershipTypeNameInput: '[data-testid="ownership-type-name-input"]',
  ownershipTypeDescriptionInput: '[data-testid="ownership-type-description-input"]',
  ownershipBuilderSave: '[data-testid="ownership-builder-save"]',
  ownershipTableDropdown: '[data-testid="ownership-table-dropdown"]',
  menuItemEdit: '[data-testid="menu-item-edit"]',
  menuItemDelete: '[data-testid="menu-item-delete"]',
} as const;

// Per-scope selector files — import from their own files for new code.
// These re-exports preserve backward compatibility for existing imports.
export { SIDEBAR } from './sidebar.selectors';
export { SCHEMA_TAB } from './schema-tab.selectors';
export { QUERIES_TAB } from './queries-tab.selectors';
export { SUMMARY_TAB } from './summary-tab.selectors';
export { DOCUMENT_SELECTORS } from './documents.selectors';
