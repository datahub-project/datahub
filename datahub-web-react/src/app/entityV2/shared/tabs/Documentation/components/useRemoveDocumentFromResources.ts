import { toast } from '@components';
import { useCallback, useEffect, useState } from 'react';

import { useUpdateDocument } from '@app/document/hooks/useUpdateDocument';
import {
    computeRelatedEntitiesForLinkChange,
    extractRelatedAssetUrns,
    extractRelatedDocumentUrns,
} from '@app/document/utils/documentUtils';

import { Document } from '@types';

interface UseRemoveDocumentFromResourcesInput {
    /** URN of the entity whose Resources section owns the pill being removed. */
    entityUrn: string | null | undefined;
    /** Current related-documents list (used to look up the doc's existing links). */
    documents: Document[];
    /** Refetch the related-documents query after the mutation lands. */
    refetch: () => Promise<unknown>;
    /** Toast copy — passed in because the caller owns the i18n namespace. */
    successMessage: string;
    errorMessage: string;
}

/**
 * Unlink a document from an entity's Resources section — the confirmation-modal
 * side of the pill's "X" affordance.
 *
 * Unlike owners/tags/terms (which read from the strongly-consistent entity query
 * and can simply `refetch()`), related documents come from an Elasticsearch-backed
 * query that lags a second or two behind writes. Naively hiding-then-unhiding on a
 * timer causes a just-removed pill to flash back in when the refetch returns
 * stale data. Instead we:
 *
 *   1. Add the URN to `removedUrns` so the pill disappears immediately and STAYS
 *      gone (the caller filters these out of the rendered list).
 *   2. Fire the mutation + a success/error toast.
 *   3. `refetch()` once so the server reconciles as soon as ES catches up.
 *   4. Prune a URN from `removedUrns` only once the server actually stops
 *      returning it (see the effect below) — never on a timer — so the pill can't
 *      reappear while the index is still catching up.
 *   5. On failure, restore the pill so the user can retry.
 */
export function useRemoveDocumentFromResources({
    entityUrn,
    documents,
    refetch,
    successMessage,
    errorMessage,
}: UseRemoveDocumentFromResourcesInput) {
    const [documentUrnToRemove, setDocumentUrnToRemove] = useState<string | null>(null);
    const [removedUrns, setRemovedUrns] = useState<Set<string>>(new Set());
    const { updateRelatedEntities } = useUpdateDocument();

    // Reconcile against the server: once a removed doc is no longer returned by the
    // query, stop tracking it. Keeping it tracked while it's still present is what
    // prevents the "removed pill flashes back in" bug during ES lag.
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

    const confirmRemove = useCallback(async () => {
        const targetUrn = documentUrnToRemove;
        if (!targetUrn || !entityUrn) return;
        const doc = documents.find((d) => d.urn === targetUrn);
        setDocumentUrnToRemove(null);
        if (!doc) return;

        // Strip the entity from whichever list holds it (relatedAssets for normal
        // entities, relatedDocuments for doc-to-doc links). The mutation replaces the
        // full list, mirroring EditableContent.tsx's handleRemoveEntity.
        const { relatedAssets, relatedDocuments } = computeRelatedEntitiesForLinkChange({
            entityUrn,
            existingAssetUrns: extractRelatedAssetUrns(doc),
            existingRelatedDocumentUrns: extractRelatedDocumentUrns(doc),
            shouldBeLinked: false,
        });

        // Optimistically drop the pill and keep it dropped until the server confirms.
        setRemovedUrns((prev) => new Set(prev).add(targetUrn));

        const ok = await updateRelatedEntities({
            urn: targetUrn,
            relatedAssets,
            relatedDocuments,
        });

        if (ok) {
            toast.success(successMessage);
            refetch();
        } else {
            setRemovedUrns((prev) => {
                if (!prev.has(targetUrn)) return prev;
                const next = new Set(prev);
                next.delete(targetUrn);
                return next;
            });
            toast.error(errorMessage);
        }
    }, [documentUrnToRemove, entityUrn, documents, updateRelatedEntities, refetch, successMessage, errorMessage]);

    return {
        /** URN currently in the confirmation modal, or null if closed. */
        documentUrnToRemove,
        /** URNs the caller should filter out of the rendered list. */
        removedUrns,
        /** Open the confirmation modal for `urn`. */
        requestRemove,
        /** Close the confirmation modal without removing anything. */
        cancelRemove,
        /** Perform the removal (called by the modal's confirm button). */
        confirmRemove,
    };
}
