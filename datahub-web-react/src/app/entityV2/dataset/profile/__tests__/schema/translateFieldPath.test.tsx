import translateFieldPath from '../../schema/utils/translateFieldPath';

describe('translateFieldPath', () => {
    it('translates qualified unions', () => {
        expect(translateFieldPath('[type=union].[type=QualifyingStruct].struct.[type=long].field')).toEqual(
            '(QualifyingStruct) struct.field',
        );
    });

    it('translates nested arrays', () => {
        expect(translateFieldPath('[type=array].[type=array].my_array.[type=long].field')).toEqual(
            'my_array[][].field',
        );
    });

    it('removes non-qualifying structs', () => {
        expect(
            translateFieldPath('[type=array].[type=array].MyArray.[type=Struct].field.[type=long].nested_field'),
        ).toEqual('MyArray[][].field.nested_field');
    });

    it('cleans the [key=true] prefix', () => {
        expect(
            translateFieldPath(
                '[key=True].[type=array].[type=array].MyArray.[type=Struct].field.[type=long].nested_field',
            ),
        ).toEqual('MyArray[][].field.nested_field');
    });

    it('leaves old fieldpaths as is', () => {
        expect(translateFieldPath('a.b.c')).toEqual('a.b.c');
    });
});
