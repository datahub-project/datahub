import { useCallback, useEffect, useMemo, useRef } from 'react';

import { RelatedDocumentExpectations, reconcileRelatedDocuments } from '@app/document/hooks/useRelatedDocuments.utils';

import {
    GetRelatedDocumentsQuery,
    RelatedDocumentsFieldsFragment,
    useGetRelatedDocumentsQuery,
} from '@graphql/document.generated';
import { Document, DocumentSourceType } from '@types';

interface RelatedDocumentsInput {
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

const RECONCILE_DELAYS_MS = [400, 800, 1500, 2500, 4000];

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

    const reconciliationGeneration = useRef(0);
    const activeReconciliation = useRef<{
        controller: AbortController;
        promise: Promise<boolean>;
    } | null>(null);
    useEffect(
        () => () => {
            reconciliationGeneration.current += 1;
            activeReconciliation.current?.controller.abort();
        },
        [entityUrn],
    );

    const reconcile = useCallback(
        async (expectations: RelatedDocumentExpectations) => {
            const generation = reconciliationGeneration.current + 1;
            reconciliationGeneration.current = generation;
            const previousReconciliation = activeReconciliation.current;
            previousReconciliation?.controller.abort();
            await previousReconciliation?.promise.catch(() => false);
            if (generation !== reconciliationGeneration.current) return false;

            const controller = new AbortController();
            const promise = reconcileRelatedDocuments({
                expectations,
                delaysMs: RECONCILE_DELAYS_MS,
                signal: controller.signal,
                fetchDocuments: async () => {
                    const result = await refetch();
                    return (extractRelatedDocuments(result?.data?.entity ?? null)?.documents ?? []) as Document[];
                },
            });
            activeReconciliation.current = { controller, promise };

            try {
                return await promise;
            } catch {
                return false;
            } finally {
                if (activeReconciliation.current?.controller === controller) {
                    activeReconciliation.current = null;
                }
            }
        },
        [refetch],
    );

    return {
        documents,
        loading,
        error,
        reconcile,
    };
}
