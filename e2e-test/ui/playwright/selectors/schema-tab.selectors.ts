export const SCHEMA_TAB = {
  schemaTab: '[data-testid="schema-tab"]',
  schemaBlamButton: '[data-testid="schema-blame-button"]',

  // Field-level
  fieldRow: (fieldName: string) => `#column-${fieldName}`,
  fieldTagsCell: (fieldName: string) => `[data-testid="schema-field-${fieldName}-tags"]`,
  fieldBlamDescription: (fieldName: string) => `[data-testid="${fieldName}-schema-blame-description"]`,

  // Field drawer
  editFieldDescription: '[data-testid="edit-field-description"]',
  fieldDescriptionEditor: '[data-testid="description-editor"] .ProseMirror',
  descriptionModalUpdateButton: '[data-testid="description-modal-update-button"]',

  // Properties drawer tab
  propertiesFieldDrawerTab: '[data-testid="Properties-field-drawer-tab-header"]',
} as const;
