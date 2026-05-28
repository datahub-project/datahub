export const QUERIES_TAB = {
  addQueryButton: '[data-testid="add-query-button"]',
  queryBuilderSaveButton: '[data-testid="query-builder-save-button"]',
  queryBuilderTitleInput: '[data-testid="query-builder-title-input"]',
  queryEditButton: (index: number) => `[data-testid="query-edit-button-${index}"]`,
  queryMoreButton: (index: number) => `[data-testid="query-more-button-${index}"]`,
  queryContent: (index: number) => `[data-testid="query-content-${index}"]`,
  sqlEditor: '[data-mode-id="sql"]',
  descriptionEditor: '.ProseMirror',
} as const;
