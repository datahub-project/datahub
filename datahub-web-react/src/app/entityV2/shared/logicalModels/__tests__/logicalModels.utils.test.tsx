import { describe, expect, it } from 'vitest';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { isLogicalModel, withLogicalModelHeaderItems } from '@app/entityV2/shared/logicalModels/logicalModels.utils';

import { EntityType } from '@types';

const logicalData = { platform: { properties: { logical: true } } } as GenericEntityProperties;
const snowflakeData = { platform: { properties: { logical: false } } } as GenericEntityProperties;

describe('logicalModels.utils', () => {
    it('isLogicalModel is true only for datasets on a platform marked logical', () => {
        expect(isLogicalModel(EntityType.Dataset, logicalData)).toBe(true);
        expect(isLogicalModel(EntityType.Dataset, snowflakeData)).toBe(false);
        expect(isLogicalModel(EntityType.Dataset, { platform: {} } as GenericEntityProperties)).toBe(false);
        expect(isLogicalModel(EntityType.Domain, logicalData)).toBe(false);
        expect(isLogicalModel(EntityType.Dataset, null)).toBe(false);
    });

    it('withLogicalModelHeaderItems adds DELETE only for logical models with the flag on', () => {
        const base = new Set([EntityMenuItems.SHARE]);

        const enabled = withLogicalModelHeaderItems(EntityType.Dataset, logicalData, base, true);
        expect(enabled?.has(EntityMenuItems.DELETE)).toBe(true);
        expect(enabled?.has(EntityMenuItems.SHARE)).toBe(true);

        // flag off -> unchanged
        expect(
            withLogicalModelHeaderItems(EntityType.Dataset, logicalData, base, false)?.has(EntityMenuItems.DELETE),
        ).toBe(false);
        // non-logical dataset -> unchanged
        expect(
            withLogicalModelHeaderItems(EntityType.Dataset, snowflakeData, base, true)?.has(EntityMenuItems.DELETE),
        ).toBe(false);
    });
});
