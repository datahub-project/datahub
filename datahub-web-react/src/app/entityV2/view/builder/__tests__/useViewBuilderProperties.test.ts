import { STRUCTURED_PROPERTY_FILTER_PREFIX } from '@app/entityV2/view/builder/constants';
import {
    buildViewBuilderProperties,
    structuredPropertyToViewProperty,
} from '@app/entityV2/view/builder/useViewBuilderProperties';
import { viewBuilderProperties } from '@app/entityV2/view/builder/viewBuilderProperties';
import { DATE_TYPE_URN, NUMBER_TYPE_URN, STRING_TYPE_URN, URN_TYPE_URN } from '@app/shared/constants';
import { STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID } from '@app/sharedV2/queryBuilder/builder/property/constants';
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

        expect(property?.id).toBe(`${STRUCTURED_PROPERTY_FILTER_PREFIX}io.acryl.dataSteward`);
        expect(property?.displayName).toBe('Data Steward');
        expect(property?.valueType).toBe(ValueTypeId.URN);
        expect(property?.valueOptions?.entityTypes).toEqual([EntityType.CorpUser, EntityType.CorpGroup]);
        expect(property?.valueOptions?.mode).toBe(SelectInputMode.MULTIPLE);
    });

    it('maps a property with allowed values to a fixed multi-select, using the description as the friendly label', () => {
        const property = structuredPropertyToViewProperty(
            makeEntity({
                qualifiedName: 'io.acryl.tier',
                valueType: { urn: STRING_TYPE_URN },
                allowedValues: [
                    { value: { stringValue: 'T1' }, description: 'Tier 1' },
                    { value: { stringValue: 'Silver' } },
                    { value: { numberValue: 3 }, description: 'Bronze' },
                    { value: { numberValue: 42 } },
                ],
            }),
        );

        expect(property?.valueType).toBe(ValueTypeId.ENUM);
        // The raw value stays the stored id so the saved filter round-trips, while the label
        // shows the property author's friendly description when set and falls back to the raw
        // value otherwise — asserted for both string and number values, with and without a
        // description.
        expect(property?.valueOptions?.options).toEqual([
            { id: 'T1', displayName: 'Tier 1' },
            { id: 'Silver', displayName: 'Silver' },
            { id: '3', displayName: 'Bronze' },
            { id: '42', displayName: '42' },
        ]);
    });

    it('maps number properties to a numeric input and date properties to a timestamp (date-picker) input', () => {
        const number = structuredPropertyToViewProperty(
            makeEntity({ qualifiedName: 'io.acryl.retentionDays', valueType: { urn: NUMBER_TYPE_URN } }),
        );
        const date = structuredPropertyToViewProperty(
            makeEntity({ qualifiedName: 'io.acryl.reviewedOn', valueType: { urn: DATE_TYPE_URN } }),
        );

        expect(number?.valueType).toBe(ValueTypeId.NUMBER);
        expect(date?.valueType).toBe(ValueTypeId.TIMESTAMP);
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

describe('buildViewBuilderProperties', () => {
    it('nests structured properties under a single Structured Property group', () => {
        const result = buildViewBuilderProperties([
            makeEntity({ qualifiedName: 'io.acryl.tier', valueType: { urn: STRING_TYPE_URN } }),
            makeEntity({ qualifiedName: 'io.acryl.retentionDays', valueType: { urn: NUMBER_TYPE_URN } }),
        ]);

        expect(result).toHaveLength(viewBuilderProperties.length + 1);
        const group = result[result.length - 1];
        expect(group.id).toBe(STRUCTURED_PROPERTY_REFERENCE_PLACEHOLDER_ID);
        expect(group.displayName).toBe('Structured Property');
        expect(group.children).toHaveLength(2);
        expect(group.children?.map((child) => child.id)).toEqual([
            `${STRUCTURED_PROPERTY_FILTER_PREFIX}io.acryl.tier`,
            `${STRUCTURED_PROPERTY_FILTER_PREFIX}io.acryl.retentionDays`,
        ]);
    });

    it('returns only the static properties when there are no structured properties', () => {
        expect(buildViewBuilderProperties([])).toBe(viewBuilderProperties);
    });
});
