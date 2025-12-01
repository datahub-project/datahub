import { useMemo } from 'react';

import {
    GetRelatedDocumentsQuery,
    RelatedDocumentsFieldsFragment,
    useGetRelatedDocumentsQuery,
} from '@graphql/document.generated';
import { Document, DocumentSourceType } from '@types';

export interface RelatedDocumentsInput {
    start?: number;
    count?: number;
    parentDocuments?: string[];
    rootOnly?: boolean;
    types?: string[];
    domains?: string[];
    sourceType?: DocumentSourceType;
}

/**
 * Type guard that checks if an entity has the relatedDocuments field.
 * This properly narrows the union type to entities that support relatedDocuments.
 */
function hasRelatedDocuments(
    entity: NonNullable<GetRelatedDocumentsQuery['entity']>,
): entity is Extract<NonNullable<GetRelatedDocumentsQuery['entity']>, { relatedDocuments?: unknown }> {
    return 'relatedDocuments' in entity;
}

/**
 * Extracts the relatedDocuments result from the entity union type.
 * Returns null if the entity doesn't have relatedDocuments or if it's null/undefined.
 *
 * This function safely extracts relatedDocuments by using a type guard to check
 * if the property exists on the entity object, which works for all entity types
 * that support relatedDocuments (Dataset, Dashboard, Chart, DataJob, DataFlow,
 * Container, MLModel, etc.)
 */
function extractRelatedDocuments(entity: GetRelatedDocumentsQuery['entity']): RelatedDocumentsFieldsFragment | null {
    if (!entity) {
        return null;
    }

    // Use type guard to narrow the type - no cast needed!
    if (hasRelatedDocuments(entity) && entity.relatedDocuments) {
        return entity.relatedDocuments;
    }

    return null;
}

export function useRelatedDocuments(entityUrn: string, input?: RelatedDocumentsInput) {
    const { data, loading, error, refetch } = useGetRelatedDocumentsQuery({
        variables: {
            urn: entityUrn,
            input: {
                start: input?.start ?? 0,
                count: input?.count ?? 100, // Default to 100 most recently updated
                parentDocuments: input?.parentDocuments,
                rootOnly: input?.rootOnly,
                types: input?.types,
                domains: input?.domains,
                sourceType: input?.sourceType,
            },
        },
        skip: !entityUrn,
        fetchPolicy: 'cache-first',
    });

    const relatedDocumentsResult = useMemo(() => {
        return extractRelatedDocuments(data?.entity ?? null);
    }, [data?.entity]);

    const documents = useMemo(() => {
        return (relatedDocumentsResult?.documents || []) as Document[];
    }, [relatedDocumentsResult]);

    const total = useMemo(() => {
        return relatedDocumentsResult?.total ?? 0;
    }, [relatedDocumentsResult]);

    return {
        documents,
        total,
        loading,
        error,
        refetch,
    };
}
