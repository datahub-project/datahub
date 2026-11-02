import { describe, expect, it } from 'vitest';

import {
    FOLDER_ALLOW,
    FOLDER_DENY,
    RESOURCE_ID_ALLOW,
    RESOURCE_ID_DENY,
    RESOURCE_NAME_ALLOW,
    RESOURCE_NAME_DENY,
} from '@app/ingestV2/source/builder/RecipeForm/sac';

describe('SAC filter field paths', () => {
    // Regression: FOLDER_ALLOW's fieldPath used to be `resource_id_pattern.allow`
    // (copy-pasted from RESOURCE_ID_ALLOW), so folder patterns silently overwrote
    // resource_id patterns and never reached the connector's folder_pattern.
    it('FOLDER_ALLOW writes to folder_pattern.allow', () => {
        expect(FOLDER_ALLOW.fieldPath).toBe('source.config.folder_pattern.allow');
    });

    it('FOLDER_DENY writes to folder_pattern.deny', () => {
        expect(FOLDER_DENY.fieldPath).toBe('source.config.folder_pattern.deny');
    });

    it('RESOURCE_ID_* patterns are isolated to resource_id_pattern', () => {
        expect(RESOURCE_ID_ALLOW.fieldPath).toBe('source.config.resource_id_pattern.allow');
        expect(RESOURCE_ID_DENY.fieldPath).toBe('source.config.resource_id_pattern.deny');
    });

    it('RESOURCE_NAME_* patterns are isolated to resource_name_pattern', () => {
        expect(RESOURCE_NAME_ALLOW.fieldPath).toBe('source.config.resource_name_pattern.allow');
        expect(RESOURCE_NAME_DENY.fieldPath).toBe('source.config.resource_name_pattern.deny');
    });
});
