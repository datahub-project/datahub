import { getDefaultTermEntities, mergeObjects } from '@app/entity/shared/entityForm/prompts/GlossaryTermsPrompt/utils';
import { glossaryTerm1, glossaryTerm3 } from '@src/Mocks';
import { EntityType, GlossaryTerm, PromptCardinality } from '@src/types.generated';

const glossaryTerm2 = {
    urn: 'urn:li:glossaryTerm:2',
    type: EntityType.GlossaryTerm,
    name: 'Another glossary term',
    hierarchicalName: 'example.AnotherGlossaryTerm',
    glossaryTermInfo: {
        name: 'Another glossary term',
        description: 'New glossary term',
        definition: 'New glossary term',
        termSource: 'termSource',
        sourceRef: 'sourceRef',
        sourceURI: 'sourceURI',
    },
    properties: {
        name: 'Another glossary term',
        description: 'New glossary term',
        definition: 'New glossary term',
        termSource: 'termSource',
        sourceRef: 'sourceRef',
        sourceURI: 'sourceURI',
    },
} as GlossaryTerm;

const existingTerms = [glossaryTerm1, glossaryTerm2, glossaryTerm3];
const allowedTerms = [glossaryTerm2, glossaryTerm3];

describe('get default term entities based on different conditions', () => {
    test('get default terms with single cardinality and unrestricted terms', () => {
        const defaultTerms = getDefaultTermEntities(existingTerms, PromptCardinality.Single, null);
        expect(defaultTerms).toEqual([glossaryTerm1]);
    });

    test('get default terms with multiple cardinality and unrestricted terms', () => {
        const defaultTerms = getDefaultTermEntities(existingTerms, PromptCardinality.Multiple, undefined);
        expect(defaultTerms).toEqual([glossaryTerm1, glossaryTerm2, glossaryTerm3]);
    });

    test('get default terms with single cardinality and restricted terms', () => {
        const defaultTerms = getDefaultTermEntities(existingTerms, PromptCardinality.Single, allowedTerms);
        expect(defaultTerms).toEqual([glossaryTerm2]);
    });

    test('get default terms with multiple cardinality and restricted terms', () => {
        const defaultTerms = getDefaultTermEntities(existingTerms, PromptCardinality.Multiple, allowedTerms);
        expect(defaultTerms).toEqual([glossaryTerm2, glossaryTerm3]);
    });

    test('get default terms when existing terms are empty', () => {
        const defaultTerms = getDefaultTermEntities([], PromptCardinality.Multiple, allowedTerms);
        expect(defaultTerms).toEqual([]);
    });

    test('get default terms when no existing term matches allowed terms', () => {
        const defaultTerms = getDefaultTermEntities([glossaryTerm1], PromptCardinality.Single, allowedTerms);
        expect(defaultTerms).toEqual([]);
    });
});

describe('merge two objects with arrays', () => {
    test('merge two objects with array properties', () => {
        const obj1 = { items: [1, 2] };
        const obj2 = { items: [3, 4] };
        const result = mergeObjects(obj1, obj2);
        expect(result).toEqual({ items: [1, 2, 3, 4] });
    });

    test('merge two objects with first one being undefined', () => {
        const obj1 = undefined;
        const obj2 = { items: [1, 2] };
        const result = mergeObjects(obj1, obj2);
        expect(result).toEqual({ items: [1, 2] });
    });

    test('merge two objects with second one being undefined', () => {
        const obj1 = { items: [1, 2] };
        const obj2 = undefined;
        const result = mergeObjects(obj1, obj2);
        expect(result).toEqual({ items: [1, 2] });
    });

    test('merge two objects with first one having undefined values', () => {
        const obj1 = { items: undefined };
        const obj2 = { items: [1, 2] };
        const result = mergeObjects(obj1, obj2);
        expect(result).toEqual({ items: [1, 2] });
    });

    test('merge two objects with second one having undefined values', () => {
        const obj1 = { items: [1, 2] };
        const obj2 = { items: undefined };
        const result = mergeObjects(obj1, obj2);
        expect(result).toEqual({ items: [1, 2] });
    });

    test('merge two objects with deeply-nested arrays', () => {
        const obj1 = { a: { b: [1] } };
        const obj2 = { a: { b: [2, 3] } };
        const result = mergeObjects(obj1, obj2);
        expect(result).toEqual({ a: { b: [1, 2, 3] } });
    });

    test('merge two objects when first is empty', () => {
        const obj2 = { items: [1, 2] };
        expect(mergeObjects({}, obj2)).toEqual(obj2);
    });

    test('merge two objects when second is empty', () => {
        const obj1 = { items: [1, 2] };
        expect(mergeObjects(obj1, {})).toEqual(obj1);
    });

    test('merge two objects with multiple array properties', () => {
        const obj1 = { items1: [1, 2], items2: ['a', 'b'] };
        const obj2 = { items1: [3, 4], items2: ['c', 'd'] };
        const result = mergeObjects(obj1, obj2);
        expect(result).toEqual({ items1: [1, 2, 3, 4], items2: ['a', 'b', 'c', 'd'] });
    });

    test('merge two objects with object properties', () => {
        const obj1 = { id: { key: 'val1' } };
        const obj2 = { id: { name: 'val2' } };
        const result = mergeObjects(obj1, obj2);
        expect(result).toEqual({ id: { key: 'val1', name: 'val2' } });
    });

    test('merge two objects with primitive values (overwrite)', () => {
        const obj1 = { id: 'value1' };
        const obj2 = { id: 'value2' };
        const result = mergeObjects(obj1, obj2);
        expect(result).toEqual({ id: 'value2' });
    });
});
