import EntityRegistry from '@src/app/entity/EntityRegistry';
import { mapStructuredPropertyToPropertyRow } from '@src/app/entity/shared/tabs/Properties/useStructuredProperties';
import {
    ENTITY_TYPES_FILTER_NAME,
    IS_HIDDEN_PROPERTY_FILTER_NAME,
    SHOW_IN_ASSET_SUMMARY_PROPERTY_FILTER_NAME,
    SHOW_IN_COLUMNS_TABLE_PROPERTY_FILTER_NAME,
} from '@src/app/search/utils/constants';
import {
    AllowedValue,
    EntityType,
    FacetFilterInput,
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
        urn: 'urn:li:dataType:datahub.string',
        label: 'Text',
        value: 'string',
        cardinality: PropertyCardinality.Single,
        description: 'A string value',
    },
    {
        urn: 'urn:li:dataType:datahub.string',
        label: 'Text - List',
        value: 'stringList',
        cardinality: PropertyCardinality.Multiple,
        description: 'A list of string values',
    },
    {
        urn: 'urn:li:dataType:datahub.number',
        label: 'Number',
        value: 'number',
        cardinality: PropertyCardinality.Single,
        description: 'An integer or decimal',
    },
    {
        urn: 'urn:li:dataType:datahub.number',
        label: 'Number - List',
        value: 'numberList',
        cardinality: PropertyCardinality.Multiple,
        description: 'A list of integers or decimals',
    },
    {
        urn: 'urn:li:dataType:datahub.urn',
        label: 'Entity',
        value: 'entity',
        cardinality: PropertyCardinality.Single,
        description: 'A reference to a DataHub asset',
    },
    {
        urn: 'urn:li:dataType:datahub.urn',
        label: 'Entity - List',
        value: 'entityList',
        cardinality: PropertyCardinality.Multiple,
        description: 'A reference to a list of DataHub assets',
    },
    {
        urn: 'urn:li:dataType:datahub.rich_text',
        label: 'Rich Text',
        value: 'richText',
        cardinality: PropertyCardinality.Single,
        description: 'A freeform string of markdown text ',
    },
    {
        urn: 'urn:li:dataType:datahub.date',
        label: 'Date',
        value: 'date',
        cardinality: PropertyCardinality.Single,
        description: 'A specific date',
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
    return values.typeQualifier?.allowedTypes?.filter((type) => !currentTypeUrns?.includes(type));
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

export const getShowInAssetSummaryPropertyFilter = () => {
    const assetSummaryFilter: FacetFilterInput = {
        field: SHOW_IN_ASSET_SUMMARY_PROPERTY_FILTER_NAME,
        values: ['true'],
    };
    return assetSummaryFilter;
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
