/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import translateFieldPath from '@app/entityV2/dataset/profile/schema/utils/translateFieldPath';

describe('translateFieldPath', () => {
    it('translates qualified unions', () => {
        expect(translateFieldPath('[type=union].[type=QualifyingStruct].struct.[type=long].field')).toEqual(
            '(QualifyingStruct) struct.field',
        );
    });

    it('translates nested arrays', () => {
        expect(translateFieldPath('[type=array].[type=array].my_array.[type=long].field')).toEqual('my_array.field');
    });

    it('removes non-qualifying structs', () => {
        expect(
            translateFieldPath('[type=array].[type=array].MyArray.[type=Struct].field.[type=long].nested_field'),
        ).toEqual('MyArray.field.nested_field');
    });

    it('cleans the [key=true] prefix', () => {
        expect(
            translateFieldPath(
                '[key=True].[type=array].[type=array].MyArray.[type=Struct].field.[type=long].nested_field',
            ),
        ).toEqual('MyArray.field.nested_field');
    });

    it('leaves old fieldpaths as is', () => {
        expect(translateFieldPath('a.b.c')).toEqual('a.b.c');
    });
});
