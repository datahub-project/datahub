import React, { useContext, useMemo, useState } from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';

export interface UpdatedDocument {
    urn: string;
    parentDocument?: string | null;
    oldParentDocument?: string | null; // Track old parent for cache invalidation
}

export interface OptimisticDocument {
    urn: string;
    title: string;
    parentDocument?: string | null;
    createdAt: number;
}

interface DocumentsContextType {
    entityData: GenericEntityProperties | null;
    setEntityData: (data: GenericEntityProperties | null) => void;
    deletedDocument: UpdatedDocument | null;
    setDeletedDocument: (document: UpdatedDocument | null) => void;
    updatedDocument: UpdatedDocument | null;
    setUpdatedDocument: (document: UpdatedDocument | null) => void;
    optimisticDocuments: OptimisticDocument[];
    addOptimisticDocument: (doc: OptimisticDocument) => void;
    removeOptimisticDocument: (urn: string) => void;
}

export const DocumentsContext = React.createContext<DocumentsContextType>({
    entityData: null,
    setEntityData: () => {},
    deletedDocument: null,
    setDeletedDocument: () => {},
    updatedDocument: null,
    setUpdatedDocument: () => {},
    optimisticDocuments: [],
    addOptimisticDocument: () => {},
    removeOptimisticDocument: () => {},
});

export const useDocumentsContext = () => {
    return useContext(DocumentsContext);
};

export const DocumentsProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
    const [entityData, setEntityData] = useState<GenericEntityProperties | null>(null);
    const [deletedDocument, setDeletedDocument] = useState<UpdatedDocument | null>(null);
    const [updatedDocument, setUpdatedDocument] = useState<UpdatedDocument | null>(null);
    const [optimisticDocuments, setOptimisticDocuments] = useState<OptimisticDocument[]>([]);

    const addOptimisticDocument = (doc: OptimisticDocument) => {
        setOptimisticDocuments((prev) => [doc, ...prev]);
    };

    const removeOptimisticDocument = (urn: string) => {
        setOptimisticDocuments((prev) => prev.filter((doc) => doc.urn !== urn));
    };

    const value = useMemo(
        () => ({
            entityData,
            setEntityData,
            deletedDocument,
            setDeletedDocument,
            updatedDocument,
            setUpdatedDocument,
            optimisticDocuments,
            addOptimisticDocument,
            removeOptimisticDocument,
        }),
        [entityData, deletedDocument, updatedDocument, optimisticDocuments],
    );

    return <DocumentsContext.Provider value={value}>{children}</DocumentsContext.Provider>;
};
