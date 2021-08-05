import React from 'react';
import translateFieldPathSegment from '../schema/utils/translateFieldPathSegment';

describe('translateFieldPathSegment', () => {
    it('translates unions', () => {
        expect(translateFieldPathSegment('[type=union]MyUnion', 0, [])).toEqual('MyUnion.');
    });

    it('translates arrays', () => {
        expect(translateFieldPathSegment('[type=array]MyArray', 0, [])).toEqual('MyArray[].');
    });

    it('translates qualifying structs at the end', () => {
        expect(
            translateFieldPathSegment('[type=QualifyingStruct]', 1, ['[type=union]MyUnion', '[type=QualifyingStruct']),
        ).toEqual(' QualifyingStruct');
    });

    it('translates qualifying structs in the middle', () => {
        expect(
            translateFieldPathSegment('[type=QualifyingStruct]', 1, [
                '[type=union]MyUnion',
                '[type=QualifyingStruct]',
                '[type=long]field',
            ]),
        ).toEqual('(QualifyingStruct) ');
    });

    it('translates primitives', () => {
        expect(
            translateFieldPathSegment('[type=long]field', 2, [
                '[type=union]MyUnion',
                '[type=QualifyingStruct]',
                '[type=long]field',
            ]),
        ).toEqual('field.');
    });
});
