export const STRUCTURED_PROPS = {
  addStructuredPropButton: '[data-testid="add-structured-prop-button"]',
  structuredPropertyStringValueInput: '[data-testid="structured-property-string-value-input"]',
  addUpdateStructuredPropButton: '[data-testid="add-update-structured-prop-on-entity-button"]',
  structuredPropEntityMoreIcon: '[data-testid="structured-prop-entity-more-icon"]',
  modalConfirmButton: '[data-testid="modal-confirm-button"]',

  // Structured properties management page
  createButton: '[data-testid="structured-props-create-button"]',
  inputName: '[data-testid="structured-props-input-name"]',
  inputDescription: '[data-testid="structured-props-input-description"]',
  selectInputType: '[data-testid="structured-props-select-input-type"]',
  propertyTypeOptionsList: '[data-testid="structured-props-property-type-options-list"]',
  selectAppliesTo: '[data-testid="structured-props-select-input-applies-to"]',
  appliesToOptionsList: '[data-testid="applies-to-options-list"]',
  createUpdateButton: '[data-testid="structured-props-create-update-button"]',
  hideSwitch: '[data-testid="structured-props-hide-switch"]',
  showInColumnsTableSwitch: '[data-testid="structured-props-show-in-columns-table-switch"]',
  showInAssetSummarySwitch: '[data-testid="structured-props-show-in-asset-summary-switch"]',
  hideInAssetSummaryWhenEmptyCheckbox: '[data-testid="structured-props-hide-in-asset-summary-when-empty-checkbox"]',
  table: '[data-testid="structured-props-table"]',
  moreOptionsIcon: '[data-testid="structured-props-more-options-icon"]',
  searchBarInput: '[data-testid="search-bar-input"]',

  addOrEditButtonForProp: (propName: string) => `[data-testid="${propName}-add-or-edit-button"]`,
} as const;
