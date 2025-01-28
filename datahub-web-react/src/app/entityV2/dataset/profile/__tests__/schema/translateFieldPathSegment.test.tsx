import translateFieldPathSegment from '../../schema/utils/translateFieldPathSegment';

describe('translateFieldPathSegment', () => {
    it('translates unions', () => {
        expect(translateFieldPathSegment('MyUnion', 1, ['[type=union]', 'MyUnion'])).toEqual('MyUnion.');
    });

    it('translates arrays', () => {
        expect(translateFieldPathSegment('MyArray', 1, ['[type=array]', 'MyArray'])).toEqual('MyArray[].');
    });

    it('translates qualifying structs in the middle', () => {
        expect(
            translateFieldPathSegment('[type=QualifyingStruct]', 1, [
                '[type=union]',
                '[type=QualifyingStruct]',
                'MyUnion',
            ]),
        ).toEqual('(QualifyingStruct) ');
    });

    it('translates qualifying structs in the end', () => {
        expect(
            translateFieldPathSegment('[type=QualifyingStruct]', 1, ['[type=union]', '[type=QualifyingStruct]']),
        ).toEqual(' QualifyingStruct');
    });

    it('translates primitives', () => {
        expect(
            translateFieldPathSegment('field', 4, [
                '[type=union]',
                'MyUnion',
                '[type=QualifyingStruct]',
                '[type=long]',
                'field',
            ]),
        ).toEqual('field.');
    });
});
