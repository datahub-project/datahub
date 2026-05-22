export const SIDEBAR = {
  container: '#entity-profile-sidebar',
  v2Container: '#entity-profile-v2-sidebar',

  // Tabs inside the sidebar panel
  tabSummary: '#entity-sidebar-tabs-tab-Summary',
  tabProperties: '#entity-sidebar-tabs-tab-Properties',
  tabLineage: '#entity-sidebar-tabs-tab-Lineage',
  tabCollapse: '#entity-sidebar-tabs-tab-collapse',

  // Section containers
  tagsSection: '#entity-profile-tags',
  glossaryTermsSection: '#entity-profile-glossary-terms',
  ownersSection: '#entity-profile-owners',
  domainsSection: '#entity-profile-domains',

  // Common section buttons
  addTagsButton: '[data-testid="add-tags-button"]',
  addTermsButton: '[data-testid="add-terms-button"]',
  addOwnersButton: '[data-testid="add-owners-button"]',
  setDomainButton: '[data-testid="set-domain-button"]',

  // Tag/term modal
  tagTermModalInput: '[data-testid="tag-term-modal-input"]',
  addTagTermFromModalBtn: '[data-testid="add-tag-term-from-modal-btn"]',

  // Domain modal
  setDomainSelect: '[data-testid="set-domain-select"]',
  submitButton: '[data-testid="submit-button"]',

  // Owner modal
  addOwnersSelect: '[data-testid="add-owners-select"]',
  addOwnersSelectBase: '[data-testid="add-owners-select-base"]',
  dropdownSearchInput: '[data-testid="dropdown-search-input"]',
  addOwnersSelectDropdown: '[data-testid="add-owners-select-dropdown"]',

  // View more / entity menu
  viewMoreButton: '[data-testid="view-more-button"]',
  entityMenuDeleteButton: '[data-testid="entity-menu-delete-button"]',
} as const;
