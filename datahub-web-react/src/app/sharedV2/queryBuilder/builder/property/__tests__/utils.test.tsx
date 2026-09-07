import { OperatorId } from '@app/sharedV2/queryBuilder/builder/property/types/operators';
import { entityProperties } from '@app/sharedV2/queryBuilder/builder/property/types/properties';
import { ValueTypeId } from '@app/sharedV2/queryBuilder/builder/property/types/values';
import { getOperatorOptions, getPropertiesForEntityTypes } from '@app/sharedV2/queryBuilder/builder/property/utils';

import { EntityType } from '@types';

describe('utils', () => {
    describe('getPropertiesForEntityTypes', () => {
        it('test single entity type', () => {
            expect(getPropertiesForEntityTypes([EntityType.Dataset])).toEqual(
                entityProperties.filter((obj) => obj.type === EntityType.Dataset)[0].properties,
            );
        });
        it('test empty entity types', () => {
            expect(getPropertiesForEntityTypes([])).toEqual([]);
        });
        it('test multiple entity type correctly intersects', () => {
            const res = getPropertiesForEntityTypes([EntityType.Dataset, EntityType.Chart, EntityType.Dashboard]);

            // Size of result should be less than both dataset props + chart props.
            expect(res.length).toBeLessThan(
                entityProperties.filter((obj) => obj.type === EntityType.Dataset)[0].properties.length,
            );
            expect(res.length).toBeLessThan(
                entityProperties.filter((obj) => obj.type === EntityType.Chart)[0].properties.length,
            );
            expect(res.length).toBeLessThan(
                entityProperties.filter((obj) => obj.type === EntityType.Dashboard)[0].properties.length,
            );
        });
    });

    describe('getOperatorOptions', () => {
        it('returns Within before Equals for hierarchical URN fields', () => {
            const options = getOperatorOptions(ValueTypeId.URN_HIERARCHY);
            expect(options?.map((op) => op.id)).toEqual([OperatorId.WITHIN, OperatorId.EQUAL_TO, OperatorId.EXISTS]);
        });

        it('does not include Within for plain URN fields', () => {
            const options = getOperatorOptions(ValueTypeId.URN);
            expect(options?.map((op) => op.id)).toEqual([OperatorId.EQUAL_TO, OperatorId.EXISTS]);
        });
    });
});
