import { describe, expect, it } from 'vitest';

import { FieldType, RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';
import { shouldStackSettingsFields } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/sections/SettingsSection.utils';

function makeField(name: string, type: FieldType): RecipeField {
    return { name, label: name, tooltip: '', type, fieldPath: `source.config.${name}`, rules: null };
}

describe('shouldStackSettingsFields', () => {
    it('should keep the grid when every field is a checkbox', () => {
        const fields = [makeField('include_tables', FieldType.BOOLEAN), makeField('include_views', FieldType.BOOLEAN)];
        expect(shouldStackSettingsFields(fields)).toBe(false);
    });

    it('should stack when any field is not a checkbox', () => {
        const fields = [makeField('sync_back_enabled', FieldType.BOOLEAN), makeField('sync_mode', FieldType.SELECT)];
        expect(shouldStackSettingsFields(fields)).toBe(true);
    });

    it('should stack when a non-checkbox field is hidden', () => {
        const fields = [
            makeField('create_repo_root_document', FieldType.BOOLEAN),
            makeField('sync_back_enabled', FieldType.BOOLEAN),
            { ...makeField('sync_back_mode', FieldType.SELECT), hidden: true },
        ];
        expect(shouldStackSettingsFields(fields)).toBe(true);
    });

    it('should keep the grid for an empty list', () => {
        expect(shouldStackSettingsFields([])).toBe(false);
    });
});
