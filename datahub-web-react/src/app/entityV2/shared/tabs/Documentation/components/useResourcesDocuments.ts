import { useCallback, useMemo } from 'react';

import { useRelatedDocuments } from '@app/document/hooks/useRelatedDocuments';
import { DocumentLinkChanges } from '@app/entityV2/shared/tabs/Documentation/components/AddContextDocumentPopover.utils';
import { useRemoveDocumentFromResources } from '@app/entityV2/shared/tabs/Documentation/components/useRemoveDocumentFromResources';

type Input = {
    entityUrn?: string | null;
    removeSuccessMessage: string;
    removeErrorMessage: string;
};

export function useResourcesDocuments({ entityUrn, removeSuccessMessage, removeErrorMessage }: Input) {
    const { documents, loading, error, reconcile } = useRelatedDocuments(entityUrn || '', { count: 100 });
    const { documentUrnToRemove, removedUrns, markRemoved, clearRemoved, requestRemove, cancelRemove, confirmRemove } =
        useRemoveDocumentFromResources({
            entityUrn,
            documents,
            reconcile,
            successMessage: removeSuccessMessage,
            errorMessage: removeErrorMessage,
        });

    const visibleDocuments = useMemo(
        () => (removedUrns.size > 0 ? documents.filter((document) => !removedUrns.has(document.urn)) : documents),
        [documents, removedUrns],
    );

    const handleDocumentsChanged = useCallback(
        ({ addedUrns, removedUrns: newlyRemovedUrns }: DocumentLinkChanges) => {
            clearRemoved(addedUrns);
            markRemoved(newlyRemovedUrns);
            reconcile({ presentUrns: addedUrns, absentUrns: newlyRemovedUrns });
        },
        [clearRemoved, markRemoved, reconcile],
    );

    const handleDocumentCreated = useCallback(
        (documentUrn: string) => {
            clearRemoved([documentUrn]);
            reconcile({ presentUrns: [documentUrn] });
        },
        [clearRemoved, reconcile],
    );

    const handleDocumentDeleted = useCallback(
        (documentUrn: string) => {
            markRemoved([documentUrn]);
            reconcile({ absentUrns: [documentUrn] });
        },
        [markRemoved, reconcile],
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
