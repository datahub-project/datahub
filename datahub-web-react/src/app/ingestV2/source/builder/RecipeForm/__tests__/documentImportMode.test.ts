import { describe, expect, it } from 'vitest';

import {
    CONFLUENCE_DOCUMENTS_IMPORT_MODE,
    GITHUB_DOCUMENTS_IMPORT_MODE,
    NOTION_DOCUMENTS_IMPORT_MODE,
} from '@app/ingestV2/source/builder/RecipeForm/documentImportMode';

describe('documentImportMode', () => {
    it('defaults GitHub to NATIVE when recipe omits document_import_mode', () => {
        const recipe = { source: { config: {} } };
        expect(GITHUB_DOCUMENTS_IMPORT_MODE.getValueFromRecipeOverride?.(recipe)).toBe('NATIVE');
    });

    it('defaults Notion to EXTERNAL when recipe omits document_import_mode', () => {
        const recipe = { source: { config: {} } };
        expect(NOTION_DOCUMENTS_IMPORT_MODE.getValueFromRecipeOverride?.(recipe)).toBe('EXTERNAL');
    });

    it('defaults Confluence to EXTERNAL when recipe omits document_import_mode', () => {
        const recipe = { source: { config: {} } };
        expect(CONFLUENCE_DOCUMENTS_IMPORT_MODE.getValueFromRecipeOverride?.(recipe)).toBe('EXTERNAL');
    });

    it('preserves explicit recipe values', () => {
        const recipe = { source: { config: { document_import_mode: 'EXTERNAL' } } };
        expect(GITHUB_DOCUMENTS_IMPORT_MODE.getValueFromRecipeOverride?.(recipe)).toBe('EXTERNAL');
    });

    it('writes Notion default EXTERNAL mode into recipe when unset', () => {
        const recipe = { source: { type: 'notion', config: {} } };
        const updated = NOTION_DOCUMENTS_IMPORT_MODE.setValueOnRecipeOverride?.(recipe, undefined);
        expect(updated.source.config.document_import_mode).toBe('EXTERNAL');
    });

    it('writes Confluence default EXTERNAL mode into recipe when unset', () => {
        const recipe = { source: { type: 'confluence', config: {} } };
        const updated = CONFLUENCE_DOCUMENTS_IMPORT_MODE.setValueOnRecipeOverride?.(recipe, undefined);
        expect(updated.source.config.document_import_mode).toBe('EXTERNAL');
    });

    it('writes NATIVE mode into Notion recipe when selected', () => {
        const recipe = { source: { type: 'notion', config: {} } };
        const updated = NOTION_DOCUMENTS_IMPORT_MODE.setValueOnRecipeOverride?.(recipe, 'NATIVE');
        expect(updated.source.config.document_import_mode).toBe('NATIVE');
    });
});
