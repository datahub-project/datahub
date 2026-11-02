import { describe, expect, it } from 'vitest';

import { getInitialValues } from '@app/entity/shared/entityForm/prompts/StructuredPropertyPrompt/utils';
import { GenericEntityProperties } from '@app/entity/shared/types';

import { FormPrompt, FormPromptType, SchemaField } from '@types';

const PROPERTY_URN = 'urn:li:structuredProperty:test';

const stringValue = (value: string) => ({ __typename: 'StringValue', stringValue: value });
const numberValue = (value: number) => ({ __typename: 'NumberValue', numberValue: value });

function buildPrompt(allowedValues?: unknown[] | null): FormPrompt {
    return {
        id: 'prompt-1',
        formUrn: 'urn:li:form:1',
        title: 'Prompt',
        type: FormPromptType.StructuredProperty,
        required: true,
        structuredPropertyParams: {
            structuredProperty: {
                urn: PROPERTY_URN,
                definition: { allowedValues: allowedValues?.map((value) => ({ value })) ?? allowedValues },
            },
        },
    } as unknown as FormPrompt;
}

function buildEntity(values: unknown[]): GenericEntityProperties {
    return {
        structuredProperties: { properties: [{ structuredProperty: { urn: PROPERTY_URN }, values }] },
    } as unknown as GenericEntityProperties;
}

describe('getInitialValues', () => {
    it('drops values that are not in allowedValues', () => {
        const prompt = buildPrompt([stringValue('Type 4 - Public'), stringValue('Type 3 - Internal')]);
        const entity = buildEntity([stringValue('Type 4 Public'), stringValue('Type 3 - Internal')]);

        expect(getInitialValues(prompt, entity)).toEqual(['Type 3 - Internal']);
    });

    it('returns an empty list when no value is allowed', () => {
        const prompt = buildPrompt([stringValue('Type 4 - Public')]);
        const entity = buildEntity([stringValue('Type 4 Public')]);

        expect(getInitialValues(prompt, entity)).toEqual([]);
    });

    it('compares values by type, as the server does', () => {
        const prompt = buildPrompt([numberValue(5)]);
        const entity = buildEntity([stringValue('5'), numberValue(5)]);

        expect(getInitialValues(prompt, entity)).toEqual([5]);
    });

    it.each([
        ['undefined', undefined],
        ['null', null],
        ['empty', []],
    ])('keeps all values when allowedValues is %s', (_, allowedValues) => {
        const prompt = buildPrompt(allowedValues);
        const entity = buildEntity([stringValue('any value'), numberValue(1)]);

        expect(getInitialValues(prompt, entity)).toEqual(['any value', 1]);
    });

    it('returns undefined when the entity has no value for the property', () => {
        expect(getInitialValues(buildPrompt([stringValue('a')]), {})).toBeUndefined();
    });

    it('filters values of a schema field', () => {
        const prompt = buildPrompt([stringValue('a')]);
        const field = {
            fieldPath: 'col_a',
            schemaFieldEntity: buildEntity([stringValue('a'), stringValue('b')]),
        } as unknown as SchemaField;

        expect(getInitialValues(prompt, null, field)).toEqual(['a']);
    });
});
