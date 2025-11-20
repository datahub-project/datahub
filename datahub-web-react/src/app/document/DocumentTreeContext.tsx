import React, { useCallback, useContext, useMemo, useState } from 'react';

/**
 * DocumentTreeContext - Single source of truth for the document tree state.
 *
 * This context manages a clean, in-memory tree structure that is completely
 * decoupled from Apollo cache. All tree operations (move, rename, delete)
 * update this state immediately, providing instant UI feedback.
 *
 * Backend mutations happen as side effects, with rollback on failure.
 * No more Apollo cache complexity or eventual consistency issues.
 */

export interface DocumentTreeNode {
    urn: string;
    title: string;
    parentUrn: string | null; // null = root
    hasChildren: boolean;
    children?: DocumentTreeNode[]; // Loaded children (undefined = not loaded yet, or merged with server)
}

interface DocumentTreeContextType {
    // Tree state
    nodes: Map<string, DocumentTreeNode>;
    rootUrns: string[];

    // Tree queries
    getNode: (urn: string) => DocumentTreeNode | undefined;
    getRootNodes: () => DocumentTreeNode[];
    getChildren: (parentUrn: string | null) => DocumentTreeNode[];

    // Tree mutations (immediate state updates)
    updateNodeTitle: (urn: string, newTitle: string) => void;
    moveNode: (urn: string, newParentUrn: string | null) => void;
    deleteNode: (urn: string) => void;
    addNode: (node: DocumentTreeNode) => void;
    setNodeChildren: (parentUrn: string | null, children: DocumentTreeNode[]) => void;

    // Batch initialization (for loading root documents or initial data)
    initializeTree: (rootNodes: DocumentTreeNode[]) => void;
}

const DocumentTreeContext = React.createContext<DocumentTreeContextType | undefined>(undefined);

export const useDocumentTree = () => {
    const context = useContext(DocumentTreeContext);
    if (!context) {
        throw new Error('useDocumentTree must be used within DocumentTreeProvider');
    }
    return context;
};

export const DocumentTreeProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
    const [nodes, setNodes] = useState<Map<string, DocumentTreeNode>>(new Map());
    const [rootUrns, setRootUrns] = useState<string[]>([]);

    // Query: Get a single node
    const getNode = useCallback(
        (urn: string) => {
            return nodes.get(urn);
        },
        [nodes],
    );

    // Query: Get all root nodes
    const getRootNodes = useCallback(() => {
        return rootUrns.map((urn) => nodes.get(urn)).filter(Boolean) as DocumentTreeNode[];
    }, [nodes, rootUrns]);

    // Query: Get children of a parent (or root if parentUrn is null)
    const getChildren = useCallback(
        (parentUrn: string | null) => {
            if (parentUrn === null) {
                return getRootNodes();
            }
            const parent = nodes.get(parentUrn);
            return parent?.children || [];
        },
        [nodes, getRootNodes],
    );

    // Mutation: Update node title
    const updateNodeTitle = useCallback((urn: string, newTitle: string) => {
        setNodes((prev) => {
            const updated = new Map(prev);
            const node = updated.get(urn);
            if (node) {
                updated.set(urn, { ...node, title: newTitle });
            }
            return updated;
        });
    }, []);

    // Mutation: Move node to a new parent
    const moveNode = useCallback(
        (urn: string, newParentUrn: string | null) => {
            setNodes((prev) => {
                const updated = new Map(prev);
                const node = updated.get(urn);
                if (!node) return prev;

                const oldParentUrn = node.parentUrn;

                // Update the node's parent
                updated.set(urn, { ...node, parentUrn: newParentUrn });

                // Update old parent's children (if loaded)
                if (oldParentUrn !== null) {
                    const oldParent = updated.get(oldParentUrn);
                    if (oldParent?.children) {
                        updated.set(oldParentUrn, {
                            ...oldParent,
                            children: oldParent.children.filter((c) => c.urn !== urn),
                            hasChildren: oldParent.children.filter((c) => c.urn !== urn).length > 0,
                        });
                    }
                }

                // Update new parent's children (if loaded)
                if (newParentUrn !== null) {
                    const newParent = updated.get(newParentUrn);
                    if (newParent?.children) {
                        updated.set(newParentUrn, {
                            ...newParent,
                            children: [{ ...node, parentUrn: newParentUrn }, ...newParent.children], // Add at top
                            hasChildren: true,
                        });
                    } else if (newParent) {
                        // Parent's children not loaded yet - initialize with this node
                        updated.set(newParentUrn, {
                            ...newParent,
                            children: [{ ...node, parentUrn: newParentUrn }],
                            hasChildren: true,
                        });
                    }
                }

                return updated;
            });

            // Update rootUrns if moving to/from root
            setRootUrns((prev) => {
                const node = nodes.get(urn);
                if (!node) return prev;

                const oldParentUrn = node.parentUrn;
                const wasRoot = oldParentUrn === null;
                const isNowRoot = newParentUrn === null;

                if (wasRoot && !isNowRoot) {
                    // Removed from root
                    return prev.filter((u) => u !== urn);
                }
                if (!wasRoot && isNowRoot) {
                    // Added to root (at top)
                    return [urn, ...prev];
                }
                return prev;
            });
        },
        [nodes],
    );

    // Mutation: Delete node
    const deleteNode = useCallback((urn: string) => {
        setNodes((prev) => {
            const updated = new Map(prev);
            const node = updated.get(urn);
            if (!node) return prev;

            // Remove from parent's children
            if (node.parentUrn !== null) {
                const parent = updated.get(node.parentUrn);
                if (parent?.children) {
                    const newChildren = parent.children.filter((c) => c.urn !== urn);
                    updated.set(node.parentUrn, {
                        ...parent,
                        children: newChildren,
                        hasChildren: newChildren.length > 0,
                    });
                }
            }

            // Remove the node itself
            updated.delete(urn);
            return updated;
        });

        // Update rootUrns if it was a root node
        setRootUrns((prev) => prev.filter((u) => u !== urn));
    }, []);

    // Mutation: Add a new node
    const addNode = useCallback((node: DocumentTreeNode) => {
        setNodes((prev) => {
            const updated = new Map(prev);
            updated.set(node.urn, node);

            // Update parent's children and hasChildren flag
            if (node.parentUrn !== null) {
                const parent = updated.get(node.parentUrn);
                if (parent) {
                    if (parent.children) {
                        // Parent has children loaded - add to existing array
                        updated.set(node.parentUrn, {
                            ...parent,
                            children: [node, ...parent.children], // Add at top
                            hasChildren: true,
                        });
                    } else {
                        // Parent exists but children not loaded - initialize children array with this node
                        updated.set(node.parentUrn, {
                            ...parent,
                            children: [node],
                            hasChildren: true,
                        });
                    }
                }
            }

            return updated;
        });

        // Add to rootUrns if it's a root node (at the top)
        if (node.parentUrn === null) {
            setRootUrns((prev) => (prev.includes(node.urn) ? prev : [node.urn, ...prev]));
        }
    }, []);

    // Mutation: Set children for a parent node
    // IMPORTANT: This merges server data with existing local state to preserve optimistic updates
    const setNodeChildren = useCallback((parentUrn: string | null, childNodes: DocumentTreeNode[]) => {
        if (parentUrn === null) {
            // Setting root nodes - merge with existing roots to preserve optimistic updates
            setRootUrns((prevRootUrns) => {
                console.log('🔄 setNodeChildren for ROOT:', {
                    existingRootCount: prevRootUrns.length,
                    serverCount: childNodes.length,
                });

                // Merge using Map for deduplication
                const mergedMap = new Map<string, string>();

                // Add server roots first (fresher data)
                childNodes.forEach((child) => {
                    mergedMap.set(child.urn, child.urn);
                });

                // Add existing roots not in server response (optimistic)
                prevRootUrns.forEach((urn) => {
                    if (!mergedMap.has(urn)) {
                        console.log('  ➕ Preserving optimistic root:', urn);
                        mergedMap.set(urn, urn);
                    }
                });

                const mergedRootUrns = Array.from(mergedMap.values());
                console.log('  ✅ Merged roots:', mergedRootUrns.length);

                return mergedRootUrns;
            });

            setNodes((prev) => {
                const updated = new Map(prev);
                childNodes.forEach((child) => updated.set(child.urn, child));
                return updated;
            });
        } else {
            // Setting children for a specific parent - merge server data with existing local children
            setNodes((prev) => {
                const updated = new Map(prev);
                const parent = updated.get(parentUrn);
                if (parent) {
                    // Get existing children (might include optimistic updates)
                    const existingChildren = parent.children || [];

                    console.log('🔄 setNodeChildren for parent:', parentUrn, {
                        existingCount: existingChildren.length,
                        serverCount: childNodes.length,
                        existingUrns: existingChildren.map((c) => c.urn),
                        serverUrns: childNodes.map((c) => c.urn),
                    });

                    // Merge strategy: Server data takes precedence, preserve optimistic updates not yet on server
                    const mergedMap = new Map<string, DocumentTreeNode>();

                    // First, add all server children (fresher data from backend)
                    childNodes.forEach((serverChild) => {
                        mergedMap.set(serverChild.urn, serverChild);
                    });

                    // Then, add existing children that are NOT in server response (optimistic updates)
                    existingChildren.forEach((existingChild) => {
                        if (!mergedMap.has(existingChild.urn)) {
                            console.log('  ➕ Preserving optimistic child:', existingChild.urn, existingChild.title);
                            mergedMap.set(existingChild.urn, existingChild);
                        }
                    });

                    // Convert back to array (guaranteed unique by URN)
                    const mergedChildren = Array.from(mergedMap.values());

                    console.log('  ✅ Merged children (deduplicated):', {
                        total: mergedChildren.length,
                        fromServer: childNodes.length,
                        optimistic: mergedChildren.length - childNodes.length,
                    });

                    updated.set(parentUrn, {
                        ...parent,
                        children: mergedChildren,
                        hasChildren: mergedChildren.length > 0,
                    });
                }
                // Also add all children to the nodes map
                childNodes.forEach((child) => updated.set(child.urn, child));
                return updated;
            });
        }
    }, []);

    // Batch initialization
    const initializeTree = useCallback((rootNodes: DocumentTreeNode[]) => {
        setRootUrns(rootNodes.map((n) => n.urn));
        setNodes(new Map(rootNodes.map((n) => [n.urn, n])));
    }, []);

    const value = useMemo(
        () => ({
            nodes,
            rootUrns,
            getNode,
            getRootNodes,
            getChildren,
            updateNodeTitle,
            moveNode,
            deleteNode,
            addNode,
            setNodeChildren,
            initializeTree,
        }),
        [
            nodes,
            rootUrns,
            getNode,
            getRootNodes,
            getChildren,
            updateNodeTitle,
            moveNode,
            deleteNode,
            addNode,
            setNodeChildren,
            initializeTree,
        ],
    );

    return <DocumentTreeContext.Provider value={value}>{children}</DocumentTreeContext.Provider>;
};
