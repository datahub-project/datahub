import { filterKeyFieldPath } from '@app/entityV2/dataset/profile/schema/utils/utils';

import { SchemaFieldDataType } from '@types';

describe('utils', () => {
    describe('filterKeyFieldPath', () => {
        it('allows keys when looking for keys', () => {
            expect(
                filterKeyFieldPath(true, {
                    fieldPath: '[version=2.0].[key=True].[type=long].field',
                    nullable: false,
                    type: SchemaFieldDataType.Number,
                    recursive: false,
                }),
            ).toEqual(true);
        });
        it('blocks non-keys when looking for keys', () => {
            expect(
                filterKeyFieldPath(true, {
                    fieldPath: '[version=2.0].[type=long].field',
                    nullable: false,
                    type: SchemaFieldDataType.Number,
                    recursive: false,
                }),
            ).toEqual(false);
        });

        it('allows non-keys when looking for non-keys', () => {
            expect(
                filterKeyFieldPath(false, {
                    fieldPath: '[version=2.0].[type=long].field',
                    nullable: false,
                    type: SchemaFieldDataType.Number,
                    recursive: false,
                }),
            ).toEqual(true);
        });

        it('blocks keys when looking for non-keys', () => {
            expect(
                filterKeyFieldPath(false, {
                    fieldPath: '[version=2.0].[key=True].[type=long].field',
                    nullable: false,
                    type: SchemaFieldDataType.Number,
                    recursive: false,
                }),
            ).toEqual(false);
        });
    });
});
