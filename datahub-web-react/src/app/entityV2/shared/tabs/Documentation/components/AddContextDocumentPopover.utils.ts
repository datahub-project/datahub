export type DocumentLinkChanges = {
    addedUrns: string[];
    removedUrns: string[];
};

export function getDocumentLinkChanges(initialUrns: Set<string>, checkedUrns: Set<string>): DocumentLinkChanges {
    return {
        addedUrns: [...checkedUrns].filter((urn) => !initialUrns.has(urn)),
        removedUrns: [...initialUrns].filter((urn) => !checkedUrns.has(urn)),
    };
}

export function getSuccessfulDocumentLinkChanges(
    changes: DocumentLinkChanges,
    mutationResults: boolean[],
): DocumentLinkChanges {
    const { addedUrns, removedUrns } = changes;
    return {
        addedUrns: addedUrns.filter((_, index) => mutationResults[index]),
        removedUrns: removedUrns.filter((_, index) => mutationResults[addedUrns.length + index]),
    };
}
