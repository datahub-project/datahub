import React from 'react';
import translateFieldPath from '../schema/translateFieldPath';

describe('translateFieldPath', () => {
    it('translates qualified unions', () => {
        expect(translateFieldPath('[type=union]MyUnion.[type=QualifyingStruct].[type=long]field')).toEqual(
            'MyUnion.(QualifyingStruct) field',
        );
    });

    it('translates nested arrays', () => {
        expect(translateFieldPath('[type=array]MyArray.[type=array].[type=long]field')).toEqual('MyArray[][].field');
    });

    it('removes non-qualifying structs', () => {
        expect(
            translateFieldPath('[type=array]MyArray.[type=array].[type=Struct]field.[type=long]nested_field'),
        ).toEqual('MyArray[][].field.nested_field');
    });

    it('cleans the [key=true] prefix', () => {
        expect(
            translateFieldPath('[key=true][type=array]MyArray.[type=array].[type=Struct]field.[type=long]nested_field'),
        ).toEqual('MyArray[][].field.nested_field');
    });
});
