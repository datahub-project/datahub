import { toast } from '@components';
import { useCallback, useEffect, useState } from 'react';

import { RelatedDocumentExpectations } from '@app/document/hooks/useRelatedDocuments.utils';
import { useUpdateDocument } from '@app/document/hooks/useUpdateDocument';
import {
    computeRelatedEntitiesForLinkChange,
    extractRelatedAssetUrns,
    extractRelatedDocumentUrns,
} from '@app/document/utils/documentUtils';

import { Document } from '@types';

type UseRemoveDocumentFromResourcesInput = {
    entityUrn: string | null | undefined;
    documents: Document[];
    reconcile: (expectations: RelatedDocumentExpectations) => Promise<boolean>;
    successMessage: string;
    errorMessage: string;
};

/**
 * Unlink a document from an entity's Resources section — the confirmation-modal
 * side of the pill's "X" affordance.
 *
 * Related documents are Elasticsearch-backed, so successful removals stay hidden
 * optimistically until reconciliation confirms that the index no longer returns them.
 */
export function useRemoveDocumentFromResources({
    entityUrn,
    documents,
    reconcile,
    successMessage,
    errorMessage,
}: UseRemoveDocumentFromResourcesInput) {
    const [documentUrnToRemove, setDocumentUrnToRemove] = useState<string | null>(null);
    const [removedUrns, setRemovedUrns] = useState<Set<string>>(new Set());
    const { updateRelatedEntities } = useUpdateDocument();

    useEffect(() => {
        setRemovedUrns((prev) => {
            if (prev.size === 0) return prev;
            const present = new Set(documents.map((d) => d.urn));
            let changed = false;
            const next = new Set(prev);
            prev.forEach((urn) => {
                if (!present.has(urn)) {
                    next.delete(urn);
                    changed = true;
                }
            });
            return changed ? next : prev;
        });
    }, [documents]);

    const requestRemove = useCallback((urn: string) => setDocumentUrnToRemove(urn), []);
    const cancelRemove = useCallback(() => setDocumentUrnToRemove(null), []);
    const markRemoved = useCallback((urns: string[]) => {
        if (urns.length === 0) return;
        setRemovedUrns((prev) => new Set([...prev, ...urns]));
    }, []);
    const clearRemoved = useCallback((urns: string[]) => {
        if (urns.length === 0) return;
        setRemovedUrns((prev) => {
            const next = new Set(prev);
            urns.forEach((urn) => next.delete(urn));
            return next.size === prev.size ? prev : next;
        });
    }, []);

    const confirmRemove = useCallback(async () => {
        const targetUrn = documentUrnToRemove;
        if (!targetUrn || !entityUrn) return;
        const doc = documents.find((d) => d.urn === targetUrn);
        setDocumentUrnToRemove(null);
        if (!doc) return;

        const { relatedAssets, relatedDocuments } = computeRelatedEntitiesForLinkChange({
            entityUrn,
            existingAssetUrns: extractRelatedAssetUrns(doc),
            existingRelatedDocumentUrns: extractRelatedDocumentUrns(doc),
            shouldBeLinked: false,
        });

        markRemoved([targetUrn]);

        const ok = await updateRelatedEntities({
            urn: targetUrn,
            relatedAssets,
            relatedDocuments,
        });

        if (ok) {
            toast.success(successMessage);
            reconcile({ absentUrns: [targetUrn] });
        } else {
            setRemovedUrns((prev) => {
                if (!prev.has(targetUrn)) return prev;
                const next = new Set(prev);
                next.delete(targetUrn);
                return next;
            });
            toast.error(errorMessage);
        }
    }, [
        documentUrnToRemove,
        entityUrn,
        documents,
        markRemoved,
        updateRelatedEntities,
        reconcile,
        successMessage,
        errorMessage,
    ]);

    return {
        documentUrnToRemove,
        removedUrns,
        markRemoved,
        clearRemoved,
        requestRemove,
        cancelRemove,
        confirmRemove,
    };
}
