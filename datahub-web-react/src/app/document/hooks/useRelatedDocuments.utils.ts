import { Document } from '@types';

export type RelatedDocumentExpectations = {
    presentUrns?: string[];
    absentUrns?: string[];
};

type ReconcileRelatedDocumentsOptions = {
    fetchDocuments: () => Promise<Document[]>;
    expectations: RelatedDocumentExpectations;
    delaysMs: number[];
    signal: AbortSignal;
};

export function doDocumentsMatchExpectations(
    documents: Pick<Document, 'urn'>[],
    { presentUrns = [], absentUrns = [] }: RelatedDocumentExpectations,
): boolean {
    const documentUrns = new Set(documents.map((document) => document.urn));
    return presentUrns.every((urn) => documentUrns.has(urn)) && absentUrns.every((urn) => !documentUrns.has(urn));
}

function waitForDelay(ms: number, signal: AbortSignal): Promise<boolean> {
    if (signal.aborted) return Promise.resolve(false);

    return new Promise((resolve) => {
        let timer: number;
        const handleAbort = () => {
            window.clearTimeout(timer);
            resolve(false);
        };
        timer = window.setTimeout(() => {
            signal.removeEventListener('abort', handleAbort);
            resolve(true);
        }, ms);
        signal.addEventListener('abort', handleAbort, { once: true });
    });
}

export async function reconcileRelatedDocuments({
    fetchDocuments,
    expectations,
    delaysMs,
    signal,
}: ReconcileRelatedDocumentsOptions): Promise<boolean> {
    const runAttempt = async (attempt: number): Promise<boolean> => {
        const waitMs = delaysMs?.[attempt];
        if (waitMs === undefined || signal.aborted) return false;
        if (!(await waitForDelay(waitMs, signal)) || signal.aborted) return false;

        const documents = await fetchDocuments();
        if (signal.aborted) return false;
        if (doDocumentsMatchExpectations(documents, expectations)) return true;
        return runAttempt(attempt + 1);
    };

    return runAttempt(0);
}
