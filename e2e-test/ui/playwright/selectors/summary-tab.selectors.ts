export const SUMMARY_TAB = {
  aboutSection: '[data-testid="about-section"]',
  descriptionViewer: '[data-testid="description-viewer"]',
  descriptionEditor: '[data-testid="description-editor"]',
  editDescriptionButton: '[data-testid="edit-description-button"]',
  publishButton: '[data-testid="publish-button"]',
  addRelatedButton: '[data-testid="add-related-button"]',
  urlInput: '[data-testid="url-input"]',
  labelInput: '[data-testid="label-input"]',
  linkFormModalSubmitButton: '[data-testid="link-form-modal-submit-button"]',

  // Properties section
  propertiesSection: '[data-testid="properties-section"]',
  propertyTitle: '[data-testid="property-title"]',
  propertyValue: '[data-testid="property-value"]',
  addPropertyButton: '[data-testid="add-property-button"]',

  propertyContainer: (type: string) => `[data-testid="property-${type}"]`,
  propertyMenuItem: (type: string) => `[data-testid="menu-item-${type}"]`,

  menuItemRemove: '[data-testid="menu-item-remove"]',
  menuItemReplace: '[data-testid="menu-item-replace"]',

  // Template section
  templateWrapper: '[data-testid="template-wrapper"]',
  module: (type: string) => `[data-testid="${type}-module"]`,
  addModule: (type: string) => `[data-testid="add-${type}-module"]`,
  moduleContent: '[data-testid="module-content"]',
  largModuleDragHandle: '[data-testid="large-module-drag-handle"]',
} as const;
