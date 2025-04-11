import { Maybe } from 'graphql/jsutils/Maybe';

import { Entity, EntityType, EntityRelationshipsResult, DataProduct, PropertyValue } from '../../../types.generated';
import { capitalizeFirstLetterOnly } from '../../shared/textUtil';
import { GenericEntityProperties } from './types';

export function dictToQueryStringParams(params: Record<string, string | boolean>) {
    return Object.keys(params)
        .map((key) => `${key}=${params[key]}`)
        .join('&');
}

export function urlEncodeUrn(urn: string) {
    return (
        urn &&
        urn
            // Hack - React Router v5 does not like pre-url-encoded paths. Since URNs can contain free form IDs, there's nothing preventing them from having percentages.
            // If we use double encoded paths, React ends up decoding them fully, which breaks our ability to read urns properly.
            .replace(/%/g, '{{encoded_percent}}')
            .replace(/\//g, '%2F')
            .replace(/\?/g, '%3F')
            .replace(/#/g, '%23')
            .replace(/\[/g, '%5B')
            .replace(/\]/g, '%5D')
    );
}

export function decodeUrn(encodedUrn: string) {
    // Hack-This is not ideal because it means that if you had the percent
    // sequence in your urn things may not work as expected.
    return decodeURIComponent(encodedUrn).replace(/{{encoded_percent}}/g, '%');
}

export function getNumberWithOrdinal(n) {
    const suffixes = ['th', 'st', 'nd', 'rd'];
    const v = n % 100;
    return n + (suffixes[(v - 20) % 10] || suffixes[v] || suffixes[0]);
}

export const encodeComma = (str: string) => {
    return str.replace(/,/g, '%2C');
};

export const decodeComma = (str: string) => {
    return str.replace(/%2C/g, ',');
};

export function notEmpty<TValue>(value: TValue | null | undefined): value is TValue {
    return value !== null && value !== undefined;
}

export const truncate = (length: number, input?: string | null) => {
    if (!input) return '';
    if (input.length > length) {
        return `${input.substring(0, length)}...`;
    }
    return input;
};

export const singularizeCollectionName = (collectionName: string): string => {
    if (!collectionName) {
        return collectionName;
    }

    const lastChar = collectionName[collectionName.length - 1];
    if (lastChar === 's') {
        return collectionName.slice(0, -1);
    }

    return collectionName;
};

export function getPlatformName(entityData: GenericEntityProperties | null) {
    return entityData?.platform?.properties?.displayName || capitalizeFirstLetterOnly(entityData?.platform?.name);
}

export const EDITED_DESCRIPTIONS_CACHE_NAME = 'editedDescriptions';

export const FORBIDDEN_URN_CHARS_REGEX = /.*[(),\\].*/;

/**
 * Utility function for checking whether a list is a subset of another.
 */
export const isListSubset = (l1, l2): boolean => {
    return l1.every((result) => l2.indexOf(result) >= 0);
};

function getGraphqlErrorCode(e) {
    if (e.graphQLErrors && e.graphQLErrors.length) {
        const firstError = e.graphQLErrors[0];
        const { extensions } = firstError;
        const errorCode = extensions && (extensions.code as number);
        return errorCode;
    }
    return undefined;
}

export const handleBatchError = (urns, e, defaultMessage) => {
    if (urns.length > 1 && getGraphqlErrorCode(e) === 403) {
        return {
            content:
                'Your bulk edit selection included entities that you are unauthorized to update. The bulk edit being performed will not be saved.',
            duration: 3,
        };
    }
    return defaultMessage;
};

// put all of the fineGrainedLineages for a given entity and its siblings into one array so we have all of it in one place
export function getFineGrainedLineageWithSiblings(
    entityData: GenericEntityProperties | null,
    getGenericEntityProperties: (type: EntityType, data: Entity) => GenericEntityProperties | null,
) {
    const fineGrainedLineages = [
        ...(entityData?.fineGrainedLineages || entityData?.inputOutput?.fineGrainedLineages || []),
    ];
    entityData?.siblingsSearch?.searchResults?.forEach((sibling) => {
        if (sibling.entity) {
            const genericSiblingProps = getGenericEntityProperties(sibling.entity.type, sibling.entity);
            if (genericSiblingProps && genericSiblingProps.fineGrainedLineages) {
                fineGrainedLineages.push(...genericSiblingProps.fineGrainedLineages);
            }
        }
    });
    return fineGrainedLineages;
}
export function getDataProduct(dataProductResult: Maybe<EntityRelationshipsResult> | undefined) {
    if (dataProductResult?.relationships && dataProductResult.relationships.length > 0) {
        return dataProductResult.relationships[0].entity as DataProduct;
    }
    return null;
}

export function getStructuredPropertyValue(value: PropertyValue) {
    if (value.__typename === 'StringValue') {
        return value.stringValue;
    }
    if (value.__typename === 'NumberValue') {
        return value.numberValue;
    }
    return null;
}

// Utility for formatting any casing of type to the expected casing for the API
export function formatEntityType(type: string): string {
    if (!type) return '';

    switch (type.toLowerCase()) {
        case 'dataset':
            return EntityType.Dataset;
        case 'role':
            return EntityType.Role;
        case 'corpuser':
            return EntityType.CorpUser;
        case 'corpgroup':
            return EntityType.CorpGroup;
        case 'dataplatform':
            return EntityType.DataPlatform;
        case 'dashboard':
            return EntityType.Dashboard;
        case 'chart':
            return EntityType.Chart;
        case 'tag':
            return EntityType.Tag;
        case 'dataflow':
            return EntityType.DataFlow;
        case 'datajob':
            return EntityType.DataJob;
        case 'glossaryterm':
            return EntityType.GlossaryTerm;
        case 'glossarynode':
            return EntityType.GlossaryNode;
        case 'mlmodel':
            return EntityType.Mlmodel;
        case 'mlmodelgroup':
            return EntityType.MlmodelGroup;
        case 'mlfeaturetable':
            return EntityType.MlfeatureTable;
        case 'mlfeature':
            return EntityType.Mlfeature;
        case 'mlprimarykey':
            return EntityType.MlprimaryKey;
        case 'container':
            return EntityType.Container;
        case 'domain':
            return EntityType.Domain;
        case 'notebook':
            return EntityType.Notebook;
        case 'dataplatforminstance':
            return EntityType.DataPlatformInstance;
        case 'test':
            return EntityType.Test;
        case 'schemafield':
            return EntityType.SchemaField;

        // these are const in the java app
        case 'dataprocessinstance': // Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME
            return EntityType.DataProcessInstance;
        case 'datahubview': // Constants.DATAHUB_VIEW_ENTITY_NAME
            return EntityType.DatahubView;
        case 'dataproduct': // Constants.DATA_PRODUCT_ENTITY_NAME
            return EntityType.DataProduct;
        case 'datahubconnection': // Constants.DATAHUB_CONNECTION_ENTITY_NAME
            return EntityType.DatahubConnection;
        case 'structuredproperty': // Constants.STRUCTURED_PROPERTY_ENTITY_NAME
            return EntityType.StructuredProperty;
        case 'assertion': // Constants.ASSERTION_ENTITY_NAME
            return EntityType.Assertion;

        default:
            return '';
    }
}

// Utility for getting entity type from urn if it's in the 3rd position
export function extractTypeFromUrn(urn: string): EntityType {
    const regex = /[^:]+:[^:]+:([^:]+):/;
    const match = urn.match(regex);

    if (match && match[1]) return formatEntityType(match[1]) as EntityType;

    return '' as EntityType;
}
