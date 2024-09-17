import { EntityType, StructuredPropertyEntity } from '@src/types.generated';

export const valueTypes = [
    {
        key: 'urn:li:dataType:datahub.string',
        label: 'String',
        value: 'string',
    },
    {
        key: 'urn:li:dataType:datahub.string',
        label: 'String - List',
        value: 'stringList',
    },
    {
        key: 'urn:li:dataType:datahub.number',
        label: 'Number',
        value: 'number',
    },
    {
        key: 'urn:li:dataType:datahub.number',
        label: 'Number - List',
        value: 'numberList',
    },
    {
        key: 'urn:li:dataType:datahub.urn',
        label: 'Entity',
        value: 'entity',
    },
    {
        key: 'urn:li:dataType:datahub.urn',
        label: 'Entity - List',
        value: 'entityList',
    },
    {
        key: 'urn:li:dataType:datahub.rich_text',
        label: 'Rich Text',
        value: 'richText',
    },
    {
        key: 'urn:li:dataType:datahub.date',
        label: 'Date',
        value: 'date',
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

export const getEntityTypeUrn = (entityType: EntityType) => {
    switch (entityType) {
        case EntityType.Dataset:
            return 'urn:li:entityType:datahub.dataset';

        case EntityType.DataJob:
            return 'urn:li:entityType:datahub.dataJob';

        case EntityType.DataFlow:
            return 'urn:li:entityType:datahub.dataFlow';

        case EntityType.Chart:
            return 'urn:li:entityType:datahub.chart';

        case EntityType.Dashboard:
            return 'urn:li:entityType:datahub.dashboard';

        case EntityType.Domain:
            return 'urn:li:entityType:datahub.domain';

        case EntityType.Container:
            return 'urn:li:entityType:datahub.container';

        case EntityType.GlossaryTerm:
            return 'urn:li:entityType:datahub.glossaryTerm';

        case EntityType.GlossaryNode:
            return 'urn:li:entityType:datahub.glossaryNode';

        case EntityType.Mlmodel:
            return 'urn:li:entityType:datahub.mlModel';

        case EntityType.MlmodelGroup:
            return 'urn:li:entityType:datahub.mlModelGroup';

        case EntityType.MlfeatureTable:
            return 'urn:li:entityType:datahub.mlFeatureTable';

        case EntityType.Mlfeature:
            return 'urn:li:entityType:datahub.mlFeature';

        case EntityType.MlprimaryKey:
            return 'urn:li:entityType:datahub.mlPrimaryKey';

        case EntityType.DataProduct:
            return 'urn:li:entityType:datahub.dataProduct';

        case EntityType.CorpUser:
            return 'urn:li:entityType:datahub.corpuser';

        case EntityType.CorpGroup:
            return 'urn:li:entityType:datahub.corpGroup';

        case EntityType.Tag:
            return 'urn:li:entityType:datahub.tag';

        case EntityType.Role:
            return 'urn:li:entityType:datahub.role';

        case EntityType.SchemaField:
            return 'urn:li:entityType:datahub.schemaField';

        default:
            return 'urn';
    }
};

export function getDisplayName(structuredProperty: StructuredPropertyEntity) {
    return structuredProperty.definition.displayName || structuredProperty.definition.qualifiedName;
}
