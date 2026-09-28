import i18next from 'i18next';

import {
    DATE_TYPE_URN,
    NUMBER_TYPE_URN,
    RICH_TEXT_TYPE_URN,
    STRING_TYPE_URN,
    URN_TYPE_URN,
} from '@app/shared/constants';
import EntityRegistry from '@src/app/entity/EntityRegistry';
import { mapStructuredPropertyToPropertyRow } from '@src/app/entity/shared/tabs/Properties/useStructuredProperties';
import {
    DISPLAY_NAME_FILTER_NAME,
    ENTITY_TYPES_FILTER_NAME,
    IS_HIDDEN_PROPERTY_FILTER_NAME,
    SHOW_IN_COLUMNS_TABLE_PROPERTY_FILTER_NAME,
    VALUE_TYPE_FIELD_NAME,
} from '@src/app/search/utils/constants';
import {
    AllowedValueInput,
    Entity,
    EntityType,
    FacetFilterInput,
    FilterOperator,
    Maybe,
    PropertyCardinality,
    SearchResult,
    StructuredProperties,
    StructuredPropertyEntity,
    StructuredPropertySettings,
} from '@src/types.generated';

/**
 * Returns true if the given structured property should be shown for an entity on the given platform.
 * If the property has no allowedPlatforms restriction, it matches any platform.
 * If platformUrn is not provided, only unrestricted properties match.
 */
export function matchesAllowedPlatforms(
    property: StructuredPropertyEntity,
    platformUrn: string | null | undefined,
): boolean {
    const allowedPlatforms = property.definition?.allowedPlatforms;
    if (!allowedPlatforms || allowedPlatforms.length === 0) {
        // No restriction — applies to all platforms
        return true;
    }
    if (!platformUrn) {
        // Property is platform-restricted but entity has no platform
        return false;
    }
    return allowedPlatforms.some((p) => p.urn === platformUrn);
}

export type StructuredProp = {
    displayName?: string;
    qualifiedName?: string;
    cardinality?: PropertyCardinality;
    description?: string | null;
    valueType?: string;
    entityTypes?: string[];
    allowedPlatforms?: string[];
    typeQualifier?: {
        allowedTypes?: string[];
    };
    immutable?: boolean;
    settings?: StructuredPropertySettings | null;
};

export const valueTypes = [
    {
        urn: STRING_TYPE_URN,
        get label() {
            return i18next.t('governance.structured-properties:valueType.textLabel');
        },
        value: 'string',
        cardinality: PropertyCardinality.Single,
        get description() {
            return i18next.t('governance.structured-properties:valueType.textDescription');
        },
    },
    {
        urn: STRING_TYPE_URN,
        get label() {
            return i18next.t('governance.structured-properties:valueType.textListLabel');
        },
        value: 'stringList',
        cardinality: PropertyCardinality.Multiple,
        get description() {
            return i18next.t('governance.structured-properties:valueType.textListDescription');
        },
    },
    {
        urn: NUMBER_TYPE_URN,
        get label() {
            return i18next.t('governance.structured-properties:valueType.numberLabel');
        },
        value: 'number',
        cardinality: PropertyCardinality.Single,
        get description() {
            return i18next.t('governance.structured-properties:valueType.numberDescription');
        },
    },
    {
        urn: NUMBER_TYPE_URN,
        get label() {
            return i18next.t('governance.structured-properties:valueType.numberListLabel');
        },
        value: 'numberList',
        cardinality: PropertyCardinality.Multiple,
        get description() {
            return i18next.t('governance.structured-properties:valueType.numberListDescription');
        },
    },
    {
        urn: URN_TYPE_URN,
        get label() {
            return i18next.t('governance.structured-properties:valueType.entityLabel');
        },
        value: 'entity',
        cardinality: PropertyCardinality.Single,
        get description() {
            return i18next.t('governance.structured-properties:valueType.entityDescription');
        },
    },
    {
        urn: URN_TYPE_URN,
        get label() {
            return i18next.t('governance.structured-properties:valueType.entityListLabel');
        },
        value: 'entityList',
        cardinality: PropertyCardinality.Multiple,
        get description() {
            return i18next.t('governance.structured-properties:valueType.entityListDescription');
        },
    },
    {
        urn: RICH_TEXT_TYPE_URN,
        get label() {
            return i18next.t('governance.structured-properties:valueType.richTextLabel');
        },
        value: 'richText',
        cardinality: PropertyCardinality.Single,
        get description() {
            return i18next.t('governance.structured-properties:valueType.richTextDescription');
        },
    },
    {
        urn: DATE_TYPE_URN,
        get label() {
            return i18next.t('governance.structured-properties:valueType.dateLabel');
        },
        value: 'date',
        cardinality: PropertyCardinality.Single,
        get description() {
            return i18next.t('governance.structured-properties:valueType.dateDescription');
        },
    },
];

export const SEARCHABLE_ENTITY_TYPES = [
    EntityType.Dataset,
    EntityType.DataJob,
    EntityType.DataFlow,
    EntityType.Chart,
    EntityType.Dashboard,
    EntityType.Domain,
    EntityType.Container,
    EntityType.GlossaryTerm,
    EntityType.GlossaryNode,
    EntityType.Mlmodel,
    EntityType.MlmodelGroup,
    EntityType.Mlfeature,
    EntityType.MlfeatureTable,
    EntityType.MlprimaryKey,
    EntityType.DataProduct,
    EntityType.CorpUser,
    EntityType.CorpGroup,
    EntityType.Tag,
    EntityType.Role,
    EntityType.Application,
];

export const APPLIES_TO_ENTITIES = [
    EntityType.Dataset,
    EntityType.DataJob,
    EntityType.DataFlow,
    EntityType.Chart,
    EntityType.Dashboard,
    EntityType.Domain,
    EntityType.Container,
    EntityType.GlossaryTerm,
    EntityType.GlossaryNode,
    EntityType.Mlmodel,
    EntityType.MlmodelGroup,
    EntityType.Mlfeature,
    EntityType.MlfeatureTable,
    EntityType.MlprimaryKey,
    EntityType.DataProduct,
    EntityType.SchemaField,
    EntityType.DataContract,
    EntityType.Application,
];

export const getEntityTypeUrn = (entityRegistry: EntityRegistry, entityType: EntityType) => {
    return `urn:li:entityType:datahub.${entityRegistry.getGraphNameFromType(entityType)}`;
};

export function getDisplayName(structuredProperty: StructuredPropertyEntity) {
    return structuredProperty.definition.displayName || structuredProperty.definition.qualifiedName;
}

export function getFilteredSortedStructuredProperties(
    properties: StructuredPropertyEntity[],
    searchQuery: string,
): StructuredPropertyEntity[] {
    const query = searchQuery.toLowerCase();
    return properties
        .filter((property) => getDisplayName(property).toLowerCase().includes(query))
        .sort((propA, propB) => (propB.definition.created?.time || 0) - (propA.definition.created?.time || 0));
}

export const getValueType = (valueUrn: string, cardinality: PropertyCardinality) => {
    return valueTypes.find((valueType) => valueType.urn === valueUrn && valueType.cardinality === cardinality)?.value;
};

export const getValueTypeLabel = (valueUrn: string, cardinality: PropertyCardinality) => {
    return valueTypes.find((valueType) => valueType.urn === valueUrn && valueType.cardinality === cardinality)?.label;
};

export const getNewAllowedTypes = (entity: StructuredPropertyEntity, values: StructuredProp) => {
    const currentTypeUrns = entity.definition.typeQualifier?.allowedTypes?.map((type) => type.urn);
    const newAllowedTypes = values.typeQualifier?.allowedTypes?.filter((type) => !currentTypeUrns?.includes(type));
    return (newAllowedTypes?.length || 0) > 0 ? newAllowedTypes : undefined;
};

export const getNewEntityTypes = (entity: StructuredPropertyEntity, values: StructuredProp) => {
    const currentTypeUrns = entity.definition.entityTypes?.map((type) => type.urn);
    return values.entityTypes?.filter((type) => !currentTypeUrns.includes(type));
};

export const getNewAllowedPlatforms = (entity: StructuredPropertyEntity, values: StructuredProp) => {
    const currentPlatformUrns = entity.definition.allowedPlatforms?.map((platform) => platform.urn);
    const newPlatforms = values.allowedPlatforms?.filter((urn) => !currentPlatformUrns?.includes(urn));
    return (newPlatforms?.length || 0) > 0 ? newPlatforms : undefined;
};

// A row the user just added has no value yet, and the live list is read on every keystroke, so
// this has to tolerate an empty or missing row.
export const getAllowedValueKey = (
    val: { numberValue?: number | string | null; stringValue?: string | null } | undefined | null,
): number | string | undefined | null => val?.numberValue ?? val?.stringValue;

/**
 * An allowed value while it is being edited. `rowId` is client-only: rows are reorderable and can
 * be blank, so neither the value nor the list position can identify a row across renders.
 */
export type AllowedValueRow = {
    rowId: string;
    isPersisted?: boolean;
    stringValue?: string | null;
    // A number row holds the raw text while the user types (e.g. "1." or "-"), coerced on submit.
    numberValue?: number | string | null;
    description?: string | null;
};

let allowedValueRowCounter = 0;

export const createAllowedValueRow = (value?: Omit<AllowedValueRow, 'rowId'>): AllowedValueRow => {
    allowedValueRowCounter += 1;
    return { ...value, rowId: `allowed-value-${allowedValueRowCounter}` };
};

export const toAllowedValueInput = ({
    stringValue,
    numberValue,
    description,
}: AllowedValueRow): AllowedValueInput | undefined => {
    const normalizedDescription = description ?? undefined;

    if (numberValue !== null && numberValue !== undefined && String(numberValue).trim() !== '') {
        const parsedNumber = Number(numberValue);
        if (Number.isFinite(parsedNumber)) {
            return { numberValue: parsedNumber, description: normalizedDescription };
        }
    }

    if (stringValue !== null && stringValue !== undefined && stringValue.trim() !== '') {
        return { stringValue, description: normalizedDescription };
    }

    return undefined;
};

/** Drops client-only row IDs and blank rows, and coerces numeric input for GraphQL. */
export const toAllowedValueInputs = (rows: AllowedValueRow[] | undefined): AllowedValueInput[] =>
    (rows ?? []).flatMap((row) => {
        const input = toAllowedValueInput(row);
        return input ? [input] : [];
    });

export const haveAllowedValuesChanged = (
    savedRows: AllowedValueRow[] | undefined,
    currentRows: AllowedValueRow[],
): boolean => JSON.stringify(toAllowedValueInputs(savedRows)) !== JSON.stringify(toAllowedValueInputs(currentRows));

export type StructuredPropertyFormErrors = {
    displayName?: string;
    valueType?: string;
    entityTypes?: string;
    qualifiedName?: string;
    /** Keyed by `AllowedValueRow.rowId`. */
    allowedValues?: Record<string, string>;
};

const NO_WHITESPACE_PATTERN = /^\S*$/;

export const validateStructuredProperty = (
    values: StructuredProp | undefined,
    allowedValueRows: AllowedValueRow[],
): StructuredPropertyFormErrors => {
    const errors: StructuredPropertyFormErrors = {};

    if (!values?.displayName?.trim()) {
        errors.displayName = i18next.t('governance.structured-properties:create.nameError');
    }
    if (!values?.valueType) {
        errors.valueType = i18next.t('governance.structured-properties:create.propertyTypeError');
    }
    if (!values?.entityTypes?.length) {
        errors.entityTypes = i18next.t('governance.structured-properties:appliesTo.error');
    }
    if (values?.qualifiedName && !NO_WHITESPACE_PATTERN.test(values.qualifiedName)) {
        errors.qualifiedName = i18next.t('governance.structured-properties:advancedOptions.qualifiedNameError');
    }

    // The list as a whole is optional, but a row the user added has to be filled in or removed.
    const blankRows = allowedValueRows.filter((row) => toAllowedValueInputs([row]).length === 0);
    if (blankRows.length) {
        errors.allowedValues = Object.fromEntries(
            blankRows.map((row) => [row.rowId, i18next.t('governance.structured-properties:allowedValues.valueError')]),
        );
    }

    return errors;
};

export const hasFormErrors = (errors: StructuredPropertyFormErrors): boolean =>
    Object.values(errors).some((error) => (typeof error === 'string' ? !!error : Object.keys(error ?? {}).length > 0));

export const getBadgeUrnToReplace = (
    existingBadgeUrn: string | undefined,
    savedPropertyUrn: string | undefined,
    enableBadge: boolean,
): string | undefined =>
    enableBadge && existingBadgeUrn && savedPropertyUrn && existingBadgeUrn !== savedPropertyUrn
        ? existingBadgeUrn
        : undefined;

type BadgeReplacement = {
    existingBadgeUrn?: string;
    savedPropertyUrn?: string;
    enableBadge: boolean;
    updateBadge: (urn: string, enabled: boolean) => Promise<unknown>;
};

export async function replaceAssetBadge({
    existingBadgeUrn,
    savedPropertyUrn,
    enableBadge,
    updateBadge,
}: BadgeReplacement): Promise<void> {
    const badgeUrnToReplace = getBadgeUrnToReplace(existingBadgeUrn, savedPropertyUrn, enableBadge);
    if (!badgeUrnToReplace || !savedPropertyUrn) return;

    try {
        await updateBadge(badgeUrnToReplace, false);
    } catch (error) {
        // Roll the newly saved property back so the previous badge remains authoritative.
        await updateBadge(savedPropertyUrn, false);
        throw error;
    }
}

export const isEntityTypeSelected = (selectedType: string) => {
    if (selectedType === 'entity' || selectedType === 'entityList') return true;
    return false;
};

export const isStringOrNumberTypeSelected = (selectedType: string) => {
    if (
        selectedType === 'string' ||
        selectedType === 'stringList' ||
        selectedType === 'number' ||
        selectedType === 'numberList'
    )
        return true;
    return false;
};

export const canBeAssetBadge = (selectedType: string, allowedValues?: AllowedValueInput[]) => {
    if (selectedType === 'string' || selectedType === 'number') {
        return (allowedValues ?? []).some((value) => {
            const key = getAllowedValueKey(value);
            return key !== undefined && key !== null && String(key).trim() !== '';
        });
    }
    return false;
};

export type PropValueField = 'stringValue' | 'numberValue';

export const getStringOrNumberValueField = (selectedType: string) => {
    if (selectedType === 'number' || selectedType === 'numberList') return 'numberValue' as PropValueField;
    return 'stringValue' as PropValueField;
};

export const getPropertyRowFromSearchResult = (
    property: SearchResult,
    structuredProperties: Maybe<StructuredProperties> | undefined,
) => {
    const entityProp = structuredProperties?.properties?.find(
        (prop) => prop.structuredProperty.urn === property.entity.urn,
    );
    return entityProp ? mapStructuredPropertyToPropertyRow(entityProp) : undefined;
};

export const getNotHiddenPropertyFilter = () => {
    const isHiddenFilter: FacetFilterInput = {
        field: IS_HIDDEN_PROPERTY_FILTER_NAME,
        values: ['true'],
        negated: true,
    };
    return isHiddenFilter;
};

export const getShowInColumnsTablePropertyFilter = () => {
    const columnsTableFilter: FacetFilterInput = {
        field: SHOW_IN_COLUMNS_TABLE_PROPERTY_FILTER_NAME,
        values: ['true'],
    };
    return columnsTableFilter;
};

export const getEntityTypesPropertyFilter = (
    entityRegistry: EntityRegistry,
    isSchemaField: boolean,
    entityType?: EntityType,
) => {
    const type = isSchemaField ? EntityType.SchemaField : entityType;

    const entityTypesFilter: FacetFilterInput = {
        field: ENTITY_TYPES_FILTER_NAME,
        values: [getEntityTypeUrn(entityRegistry, type || EntityType.SchemaField)],
    };
    return entityTypesFilter;
};

export const getValueTypeFilter = (valueTypeUrns: string[]) => {
    const valueTypeFilter: FacetFilterInput = {
        field: VALUE_TYPE_FIELD_NAME,
        values: valueTypeUrns,
    };
    return valueTypeFilter;
};

export const getDisplayNameFilter = (displayNameQuery: string) => {
    const displayNameFilter: FacetFilterInput = {
        field: DISPLAY_NAME_FILTER_NAME,
        condition: FilterOperator.Contain,
        values: [displayNameQuery],
    };
    return displayNameFilter;
};

export function isStructuredProperty(entity?: Entity | null | undefined): entity is StructuredPropertyEntity {
    return !!entity && entity.type === EntityType.StructuredProperty;
}

export function getStructuredPropertiesSearchInputs(
    entityRegistry: EntityRegistry,
    entityType: EntityType,
    fieldUrn?: string,
    nameQuery?: string,
) {
    return {
        types: [EntityType.StructuredProperty],
        query: '*',
        start: 0,
        count: 100,
        searchFlags: { skipCache: true },
        orFilters: [
            {
                and: [
                    getEntityTypesPropertyFilter(entityRegistry, !!fieldUrn, entityType),
                    getNotHiddenPropertyFilter(),
                    ...(nameQuery ? [getDisplayNameFilter(nameQuery)] : []),
                ],
            },
        ],
    };
}
