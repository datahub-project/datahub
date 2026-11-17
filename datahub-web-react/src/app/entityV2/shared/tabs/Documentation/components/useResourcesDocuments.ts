import { useCallback, useEffect, useMemo, useState } from 'react';

import { useRelatedDocuments } from '@app/document/hooks/useRelatedDocuments';
import { DocumentLinkChanges } from '@app/entityV2/shared/tabs/Documentation/components/AddContextDocumentPopover.utils';
import { useRemoveDocumentFromResources } from '@app/entityV2/shared/tabs/Documentation/components/useRemoveDocumentFromResources';
import {
    createPendingResourceDocument,
    mergeResourcesDocuments,
    pruneAddedDocuments,
    upsertAddedDocuments,
} from '@app/entityV2/shared/tabs/Documentation/components/useResourcesDocuments.utils';

import { Document } from '@types';

type Input = {
    entityUrn?: string | null;
    removeSuccessMessage: string;
    removeErrorMessage: string;
};

export function useResourcesDocuments({ entityUrn, removeSuccessMessage, removeErrorMessage }: Input) {
    const { documents, loading, error } = useRelatedDocuments(entityUrn || '', { count: 100 });
    const [addedDocuments, setAddedDocuments] = useState<Document[]>([]);
    const documentsForActions = useMemo(
        () => upsertAddedDocuments(documents, addedDocuments),
        [documents, addedDocuments],
    );
    const { documentUrnToRemove, removedUrns, markRemoved, clearRemoved, requestRemove, cancelRemove, confirmRemove } =
        useRemoveDocumentFromResources({
            entityUrn,
            documents: documentsForActions,
            successMessage: removeSuccessMessage,
            errorMessage: removeErrorMessage,
        });

    useEffect(() => {
        setAddedDocuments([]);
    }, [entityUrn]);

    useEffect(() => {
        const fetchedUrns = new Set(documents.map((document) => document.urn));
        setAddedDocuments((prev) => pruneAddedDocuments(prev, fetchedUrns));
    }, [documents]);

    useEffect(() => {
        if (removedUrns.size === 0) return;
        setAddedDocuments((prev) => {
            const next = prev.filter((document) => !removedUrns.has(document.urn));
            return next.length === prev.length ? prev : next;
        });
    }, [removedUrns]);

    const visibleDocuments = useMemo(
        () => mergeResourcesDocuments({ fetched: documents, added: addedDocuments, removedUrns }),
        [documents, addedDocuments, removedUrns],
    );

    const handleDocumentsChanged = useCallback(
        ({ addedDocuments: newlyAdded, removedUrns: newlyRemovedUrns }: DocumentLinkChanges) => {
            clearRemoved(newlyAdded.map((document) => document.urn));
            markRemoved(newlyRemovedUrns);
            setAddedDocuments((prev) => upsertAddedDocuments(prev, newlyAdded));
        },
        [clearRemoved, markRemoved],
    );

    const handleDocumentCreated = useCallback(
        (documentUrn: string) => {
            if (!entityUrn) return;
            clearRemoved([documentUrn]);
            setAddedDocuments((prev) =>
                upsertAddedDocuments(prev, [createPendingResourceDocument(documentUrn, entityUrn)]),
            );
        },
        [clearRemoved, entityUrn],
    );

    const handleDocumentDeleted = useCallback(
        (documentUrn: string) => {
            markRemoved([documentUrn]);
        },
        [markRemoved],
    );

    return {
        visibleDocuments,
        loading,
        error,
        documentUrnToRemove,
        requestRemove,
        cancelRemove,
        confirmRemove,
        handleDocumentsChanged,
        handleDocumentCreated,
        handleDocumentDeleted,
    };
}
