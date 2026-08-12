import { STRUCTURED_PROPERTY_FILTER_PREFIX } from '@app/entityV2/view/builder/constants';
import { structuredPropertyToViewProperty } from '@app/entityV2/view/builder/useViewBuilderProperties';
import { DATE_TYPE_URN, NUMBER_TYPE_URN, STRING_TYPE_URN, URN_TYPE_URN } from '@app/shared/constants';
import { SelectInputMode, ValueTypeId } from '@app/sharedV2/queryBuilder/builder/property/types/values';

import { EntityType, StructuredPropertyEntity } from '@types';

function makeEntity(definition: Record<string, unknown> | undefined): StructuredPropertyEntity {
    return {
        urn: 'urn:li:structuredProperty:test',
        type: EntityType.StructuredProperty,
        definition,
    } as unknown as StructuredPropertyEntity;
}

describe('structuredPropertyToViewProperty', () => {
    it('maps a URN-valued property to an entity-search property scoped to its allowed types', () => {
        const property = structuredPropertyToViewProperty(
            makeEntity({
                qualifiedName: 'io.acryl.dataSteward',
                displayName: 'Data Steward',
                valueType: { urn: URN_TYPE_URN },
                typeQualifier: { allowedTypes: [{ type: EntityType.CorpUser }, { type: EntityType.CorpGroup }] },
            }),
        );

        // Field id matches the search facet field so the saved View filter round-trips.
        expect(property?.id).toBe(`${STRUCTURED_PROPERTY_FILTER_PREFIX}io.acryl.dataSteward`);
        expect(property?.displayName).toBe('Data Steward');
        expect(property?.valueType).toBe(ValueTypeId.URN);
        expect(property?.valueOptions?.entityTypes).toEqual([EntityType.CorpUser, EntityType.CorpGroup]);
        expect(property?.valueOptions?.mode).toBe(SelectInputMode.MULTIPLE);
    });

    it('maps a property with allowed values to a fixed multi-select', () => {
        const property = structuredPropertyToViewProperty(
            makeEntity({
                qualifiedName: 'io.acryl.tier',
                valueType: { urn: STRING_TYPE_URN },
                allowedValues: [
                    { value: { stringValue: 'Gold' } },
                    { value: { stringValue: 'Silver' } },
                    { value: { numberValue: 3 } },
                ],
            }),
        );

        expect(property?.valueType).toBe(ValueTypeId.ENUM);
        expect(property?.valueOptions?.options).toEqual([
            { id: 'Gold', displayName: 'Gold' },
            { id: 'Silver', displayName: 'Silver' },
            { id: '3', displayName: '3' },
        ]);
    });

    it('maps number and date properties to a numeric input', () => {
        const number = structuredPropertyToViewProperty(
            makeEntity({ qualifiedName: 'io.acryl.retentionDays', valueType: { urn: NUMBER_TYPE_URN } }),
        );
        const date = structuredPropertyToViewProperty(
            makeEntity({ qualifiedName: 'io.acryl.reviewedOn', valueType: { urn: DATE_TYPE_URN } }),
        );

        expect(number?.valueType).toBe(ValueTypeId.NUMBER);
        expect(date?.valueType).toBe(ValueTypeId.NUMBER);
    });

    it('falls back to a text input for free-form string properties', () => {
        const property = structuredPropertyToViewProperty(
            makeEntity({ qualifiedName: 'io.acryl.notes', valueType: { urn: STRING_TYPE_URN } }),
        );

        expect(property?.valueType).toBe(ValueTypeId.STRING);
    });

    it('skips properties without a qualified name', () => {
        expect(structuredPropertyToViewProperty(makeEntity({ displayName: 'No Name' }))).toBeUndefined();
        expect(structuredPropertyToViewProperty(makeEntity(undefined))).toBeUndefined();
    });
});
