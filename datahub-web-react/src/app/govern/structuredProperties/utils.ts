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
    AllowedValue,
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

export type StructuredProp = {
    displayName?: string;
    qualifiedName?: string;
    cardinality?: PropertyCardinality;
    description?: string | null;
    valueType?: string;
    entityTypes?: string[];
    typeQualifier?: {
        allowedTypes?: string[];
    };
    immutable?: boolean;
    allowedValues?: AllowedValue[];
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

export const getNewAllowedValues = (entity: StructuredPropertyEntity, values: StructuredProp) => {
    const currentAllowedValues = entity.definition.allowedValues?.map(
        (val: any) => val.value.numberValue || val.value.stringValue,
    );
    return values.allowedValues?.filter(
        (val: any) =>
            !(currentAllowedValues?.includes(val.stringValue) || currentAllowedValues?.includes(val.numberValue)),
    );
};

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

export const canBeAssetBadge = (selectedType: string, allowedValues?: AllowedValue[]) => {
    if (selectedType === 'string' || selectedType === 'number') {
        return !!allowedValues?.length;
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
