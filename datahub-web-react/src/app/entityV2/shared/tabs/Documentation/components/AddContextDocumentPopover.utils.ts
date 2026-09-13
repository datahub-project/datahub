import { Document } from '@types';

export type DocumentLinkDiff = {
    addedUrns: string[];
    removedUrns: string[];
};

export type DocumentLinkChanges = {
    addedDocuments: Document[];
    removedUrns: string[];
};

export function getDocumentLinkChanges(initialUrns: Set<string>, checkedUrns: Set<string>): DocumentLinkDiff {
    return {
        addedUrns: [...checkedUrns].filter((urn) => !initialUrns.has(urn)),
        removedUrns: [...initialUrns].filter((urn) => !checkedUrns.has(urn)),
    };
}

export type ApplyLinkResult = {
    ok: boolean;
    document: Document | null;
};

export function getSuccessfulDocumentLinkChanges(
    addedResults: ApplyLinkResult[],
    removedUrns: string[],
    removedResults: ApplyLinkResult[],
): DocumentLinkChanges {
    return {
        addedDocuments: addedResults
            .filter(
                (result): result is ApplyLinkResult & { document: Document } => result.ok && result.document != null,
            )
            .map((result) => result.document),
        removedUrns: removedUrns.filter((_, index) => removedResults[index]?.ok),
    };
}
