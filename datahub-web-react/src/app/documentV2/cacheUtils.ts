import { ApolloClient } from '@apollo/client';

import { GetDocumentDocument, SearchDocumentsDocument, SearchDocumentsQuery } from '@graphql/document.generated';
import { DocumentState } from '@types';

/**
 * Remove a document from the searchDocuments cache for a specific parent.
 * This is used when a document is moved or deleted.
 */
export const removeDocumentFromSearchCache = (
    client: ApolloClient<any>,
    documentUrn: string,
    parentDocumentUrn?: string | null,
) => {
    console.log('🗑️ removeDocumentFromSearchCache called:', {
        documentUrn,
        parentDocumentUrn: parentDocumentUrn || 'ROOT',
    });

    try {
        // For nested documents (has a parent)
        if (parentDocumentUrn) {
            const currData: SearchDocumentsQuery | null = client.readQuery({
                query: SearchDocumentsDocument,
                variables: {
                    input: {
                        query: '*',
                        parentDocument: parentDocumentUrn,
                        states: [DocumentState.Published, DocumentState.Unpublished],
                        includeDrafts: false,
                        start: 0,
                        count: 100,
                    },
                },
            });

            if (currData?.searchDocuments?.documents) {
                const newDocuments = currData.searchDocuments.documents.filter((doc) => doc.urn !== documentUrn);

                client.writeQuery({
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            query: '*',
                            parentDocument: parentDocumentUrn,
                            states: [DocumentState.Published, DocumentState.Unpublished],
                            includeDrafts: false,
                            start: 0,
                            count: 100,
                        },
                    },
                    data: {
                        searchDocuments: {
                            ...currData.searchDocuments,
                            total: Math.max((currData.searchDocuments.total || 1) - 1, 0),
                            documents: newDocuments,
                        },
                    },
                });
            }
        }
        // For root documents (no parent)
        else {
            const currData: SearchDocumentsQuery | null = client.readQuery({
                query: SearchDocumentsDocument,
                variables: {
                    input: {
                        query: '*',
                        rootOnly: true,
                        states: [DocumentState.Published, DocumentState.Unpublished],
                        includeDrafts: false,
                        start: 0,
                        count: 100,
                    },
                },
            });

            if (currData?.searchDocuments?.documents) {
                const newDocuments = currData.searchDocuments.documents.filter((doc) => doc.urn !== documentUrn);

                client.writeQuery({
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            query: '*',
                            rootOnly: true,
                            states: [DocumentState.Published, DocumentState.Unpublished],
                            includeDrafts: false,
                            start: 0,
                            count: 100,
                        },
                    },
                    data: {
                        searchDocuments: {
                            ...currData.searchDocuments,
                            total: Math.max((currData.searchDocuments.total || 1) - 1, 0),
                            documents: newDocuments,
                        },
                    },
                });
            }
        }
    } catch (error) {
        // Cache entry might not exist yet, which is fine
        console.error('❌ Failed to remove document from search cache:', error, {
            documentUrn,
            parentDocumentUrn,
        });
    }
};

/**
 * Add a document to the searchDocuments cache for a specific parent.
 * This is used when a document is moved or created.
 */
export const addDocumentToSearchCache = (
    client: ApolloClient<any>,
    document: any,
    parentDocumentUrn?: string | null,
) => {
    console.log('🔧 addDocumentToSearchCache called:', {
        documentUrn: document?.urn,
        documentTitle: document?.info?.title,
        parentDocumentUrn,
    });

    try {
        // For nested documents (has a parent)
        if (parentDocumentUrn) {
            let currData: SearchDocumentsQuery | null = null;
            try {
                currData = client.readQuery({
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            query: '*',
                            parentDocument: parentDocumentUrn,
                            states: [DocumentState.Published, DocumentState.Unpublished],
                            includeDrafts: false,
                            start: 0,
                            count: 100,
                        },
                    },
                });
            } catch (e) {
                // Cache miss is fine - we'll create a new entry
            }

            // Determine new documents array
            let newDocuments;
            let newTotal;
            let existingIndex = -1;

            if (currData?.searchDocuments?.documents) {
                // Cache entry exists - update it
                existingIndex = currData.searchDocuments.documents.findIndex((doc) => doc.urn === document.urn);

                if (existingIndex >= 0) {
                    // Update existing document
                    newDocuments = [...currData.searchDocuments.documents];
                    newDocuments[existingIndex] = document;
                    newTotal = currData.searchDocuments.total;
                } else {
                    // Add new document to the beginning
                    newDocuments = [document, ...currData.searchDocuments.documents];
                    newTotal = (currData.searchDocuments.total || 0) + 1;
                }
            } else {
                // No cache entry - create one with this document as the first child
                newDocuments = [document];
                newTotal = 1;
            }

            // Always write to cache (create or update)
            // Determine the action for logging
            let action = 'CREATE';
            if (existingIndex >= 0) {
                action = 'UPDATE';
            } else if (currData) {
                action = 'ADD';
            }

            console.log('💾 Writing to Apollo cache (NESTED):', {
                parentDocumentUrn,
                newTotal,
                documentsCount: newDocuments.length,
                documents: newDocuments.map((d: any) => ({ urn: d.urn, title: d.info?.title })),
                action,
            });

            client.writeQuery({
                query: SearchDocumentsDocument,
                variables: {
                    input: {
                        query: '*',
                        parentDocument: parentDocumentUrn,
                        states: [DocumentState.Published, DocumentState.Unpublished],
                        includeDrafts: false,
                        start: 0,
                        count: 100,
                    },
                },
                data: {
                    searchDocuments: {
                        __typename: 'SearchDocumentsResult',
                        total: newTotal,
                        start: 0,
                        count: newDocuments.length,
                        documents: newDocuments,
                        facets: [], // Required by Apollo cache - empty array if not used
                    },
                },
            });

            console.log('✅ Apollo cache write complete');
        }
        // For root documents (no parent)
        else {
            let currData: SearchDocumentsQuery | null = null;
            try {
                currData = client.readQuery({
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            query: '*',
                            rootOnly: true,
                            states: [DocumentState.Published, DocumentState.Unpublished],
                            includeDrafts: false,
                            start: 0,
                            count: 100,
                        },
                    },
                });
            } catch (e) {
                console.log('⚠️ Root cache miss, will create new entry');
            }

            // Determine new documents array
            let newDocuments;
            let newTotal;

            if (currData?.searchDocuments?.documents) {
                // Check if document already exists in cache
                const existingIndex = currData.searchDocuments.documents.findIndex((doc) => doc.urn === document.urn);

                if (existingIndex >= 0) {
                    // Update existing document
                    newDocuments = [...currData.searchDocuments.documents];
                    newDocuments[existingIndex] = document;
                    newTotal = currData.searchDocuments.total;
                } else {
                    // Add new document to the beginning
                    newDocuments = [document, ...currData.searchDocuments.documents];
                    newTotal = (currData.searchDocuments.total || 0) + 1;
                }
            } else {
                // No cache entry - create one with this document as the first root doc
                newDocuments = [document];
                newTotal = 1;
            }

            console.log('💾 Writing to Apollo cache (ROOT):', {
                newTotal,
                documentsCount: newDocuments.length,
                documents: newDocuments.map((d: any) => ({ urn: d.urn, title: d.info?.title })),
                action: currData ? 'UPDATE' : 'CREATE',
            });

            client.writeQuery({
                query: SearchDocumentsDocument,
                variables: {
                    input: {
                        query: '*',
                        rootOnly: true,
                        states: [DocumentState.Published, DocumentState.Unpublished],
                        includeDrafts: false,
                        start: 0,
                        count: 100,
                    },
                },
                data: {
                    searchDocuments: {
                        __typename: 'SearchDocumentsResult',
                        total: newTotal,
                        start: 0,
                        count: newDocuments.length,
                        documents: newDocuments,
                        facets: [], // Required by Apollo cache
                    },
                },
            });

            console.log('✅ Apollo cache write complete (ROOT)');
        }
    } catch (error) {
        // Cache entry might not exist yet, which is fine
        console.error('❌ Failed to add document to search cache:', error);
    }
};

/**
 * Update a document's title in all relevant Apollo cache entries.
 * This updates both the searchDocuments cache and the individual document cache.
 */
export const updateDocumentTitleInCache = (
    client: ApolloClient<any>,
    documentUrn: string,
    newTitle: string,
    parentDocumentUrn?: string | null,
) => {
    console.log('✏️ updateDocumentTitleInCache called:', {
        documentUrn,
        newTitle,
        parentDocumentUrn: parentDocumentUrn || 'ROOT',
    });

    try {
        // Update in searchDocuments cache (sidebar tree)
        if (parentDocumentUrn) {
            // Nested document
            const currData: SearchDocumentsQuery | null = client.readQuery({
                query: SearchDocumentsDocument,
                variables: {
                    input: {
                        query: '*',
                        parentDocument: parentDocumentUrn,
                        states: [DocumentState.Published, DocumentState.Unpublished],
                        includeDrafts: false,
                        start: 0,
                        count: 100,
                    },
                },
            });

            if (currData?.searchDocuments?.documents) {
                const updatedDocuments = currData.searchDocuments.documents.map((doc) =>
                    doc.urn === documentUrn
                        ? {
                              ...doc,
                              info: {
                                  ...doc.info,
                                  title: newTitle,
                              },
                          }
                        : doc,
                );

                client.writeQuery({
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            query: '*',
                            parentDocument: parentDocumentUrn,
                            states: [DocumentState.Published, DocumentState.Unpublished],
                            includeDrafts: false,
                            start: 0,
                            count: 100,
                        },
                    },
                    data: {
                        searchDocuments: {
                            ...currData.searchDocuments,
                            documents: updatedDocuments,
                        },
                    },
                });
                console.log('✅ Updated title in nested searchDocuments cache');
            }
        } else {
            // Root document
            const currData: SearchDocumentsQuery | null = client.readQuery({
                query: SearchDocumentsDocument,
                variables: {
                    input: {
                        query: '*',
                        parentDocument: null,
                        states: [DocumentState.Published, DocumentState.Unpublished],
                        includeDrafts: false,
                        start: 0,
                        count: 100,
                    },
                },
            });

            if (currData?.searchDocuments?.documents) {
                const updatedDocuments = currData.searchDocuments.documents.map((doc) =>
                    doc.urn === documentUrn
                        ? {
                              ...doc,
                              info: {
                                  ...doc.info,
                                  title: newTitle,
                              },
                          }
                        : doc,
                );

                client.writeQuery({
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            query: '*',
                            parentDocument: null,
                            states: [DocumentState.Published, DocumentState.Unpublished],
                            includeDrafts: false,
                            start: 0,
                            count: 100,
                        },
                    },
                    data: {
                        searchDocuments: {
                            ...currData.searchDocuments,
                            documents: updatedDocuments,
                        },
                    },
                });
                console.log('✅ Updated title in root searchDocuments cache');
            }
        }

        // Also update the individual document cache (if it exists)
        try {
            const documentData = client.readQuery({
                query: GetDocumentDocument,
                variables: { urn: documentUrn },
            });

            if (documentData?.document) {
                client.writeQuery({
                    query: GetDocumentDocument,
                    variables: { urn: documentUrn },
                    data: {
                        document: {
                            ...documentData.document,
                            info: {
                                ...documentData.document.info,
                                title: newTitle,
                            },
                        },
                    },
                });
                console.log('✅ Updated title in GetDocument cache');
            }
        } catch (e) {
            // GetDocument might not be cached, that's fine
            console.log('ℹ️ GetDocument not in cache (expected if not on detail page)');
        }
    } catch (error) {
        console.error('❌ Failed to update document title in cache:', error);
    }
};
