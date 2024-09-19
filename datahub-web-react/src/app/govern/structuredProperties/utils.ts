import EntityRegistry from '@src/app/entityV2/EntityRegistry';
import { EntityType, PropertyCardinality, StructuredPropertyEntity } from '@src/types.generated';

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
};

export const valueTypes = [
    {
        urn: 'urn:li:dataType:datahub.string',
        label: 'String',
        value: 'string',
        cardinality: PropertyCardinality.Single,
    },
    {
        urn: 'urn:li:dataType:datahub.string',
        label: 'String - List',
        value: 'stringList',
        cardinality: PropertyCardinality.Multiple,
    },
    {
        urn: 'urn:li:dataType:datahub.number',
        label: 'Number',
        value: 'number',
        cardinality: PropertyCardinality.Single,
    },
    {
        urn: 'urn:li:dataType:datahub.number',
        label: 'Number - List',
        value: 'numberList',
        cardinality: PropertyCardinality.Multiple,
    },
    {
        urn: 'urn:li:dataType:datahub.urn',
        label: 'Entity',
        value: 'entity',
        cardinality: PropertyCardinality.Single,
    },
    {
        urn: 'urn:li:dataType:datahub.urn',
        label: 'Entity - List',
        value: 'entityList',
        cardinality: PropertyCardinality.Multiple,
    },
    {
        urn: 'urn:li:dataType:datahub.rich_text',
        label: 'Rich Text',
        value: 'richText',
        cardinality: PropertyCardinality.Single,
    },
    {
        urn: 'urn:li:dataType:datahub.date',
        label: 'Date',
        value: 'date',
        cardinality: PropertyCardinality.Single,
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

export const getNewAllowedTypes = (entity: StructuredPropertyEntity, values: StructuredProp) => {
    const currentTypeUrns = entity.definition.typeQualifier?.allowedTypes?.map((type) => type.urn);
    return values.typeQualifier?.allowedTypes?.filter((type) => !currentTypeUrns?.includes(type));
};

export const getNewEntityTypes = (entity: StructuredPropertyEntity, values: StructuredProp) => {
    const currentTypeUrns = entity.definition.entityTypes?.map((type) => type.urn);
    return values.entityTypes?.filter((type) => !currentTypeUrns.includes(type));
};

export const isEntityTypeSelected = (selectedType: string) => {
    if (selectedType === 'urn:li:dataType:datahub.urn') return true;
    return false;
};
