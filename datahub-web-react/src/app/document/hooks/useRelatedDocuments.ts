import { useMemo } from 'react';

import { useGetRelatedDocumentsQuery } from '@graphql/document.generated';
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

    // Extract relatedDocuments from the entity based on its type
    // The GraphQL query uses fragments for each entity type, so we need to check all possible types
    const relatedDocumentsResult = useMemo(() => {
        if (!data?.entity) {
            return null;
        }

        // The entity could be any type that supports relatedDocuments
        // Use a type guard to safely check if relatedDocuments exists
        // (Some entity types have relatedDocuments, others don't)
        const { entity } = data;
        if ('relatedDocuments' in entity && entity.relatedDocuments) {
            return entity.relatedDocuments;
        }
        return null;
    }, [data]);

    const documents = useMemo(() => {
        return (relatedDocumentsResult?.documents || []) as Document[];
    }, [relatedDocumentsResult]);

    const total = useMemo(() => {
        return relatedDocumentsResult?.total || 0;
    }, [relatedDocumentsResult]);

    return {
        documents,
        total,
        loading,
        error,
        refetch,
    };
}
