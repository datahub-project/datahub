import React, { useCallback, useState } from 'react';
import { useHistory } from 'react-router-dom';

import { useCreateDocumentTreeMutation } from '@app/document/hooks/useDocumentTreeMutations';
import { useNavBarContext } from '@app/homeV2/layout/navBarRedesign/NavBarContext';
import { NavBarMenuGroup, NavBarMenuItemTypes } from '@app/homeV2/layout/navBarRedesign/types';
import { ContextGroupHeader } from '@app/homeV2/layout/sidebar/documents/ContextGroupHeader';
import { DocumentTree } from '@app/homeV2/layout/sidebar/documents/DocumentTree';
import { SearchDocumentPopover } from '@app/homeV2/layout/sidebar/documents/SearchDocumentPopover';
import { useIsContextDocumentsEnabled } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

export function useContextMenuItems(): NavBarMenuGroup | null {
    const isContextBaseEnabled = useIsContextDocumentsEnabled();
    const { isCollapsed } = useNavBarContext();
    const [creating, setCreating] = useState(false);
    const [showSearchPopover, setShowSearchPopover] = useState(false);
    const { createDocument } = useCreateDocumentTreeMutation();
    const history = useHistory();
    const entityRegistry = useEntityRegistry();

    const handleCreateDocument = useCallback(
        async (parentDocumentUrn?: string) => {
            setCreating(true);
            try {
                const newUrn = await createDocument({
                    title: 'New Document',
                    parentDocument: parentDocumentUrn || null,
                    // No subType - let users choose after creation
                });

                // Navigate to the new document
                if (newUrn) {
                    const url = entityRegistry.getEntityUrl(EntityType.Document, newUrn);
                    history.push(url);
                }
            } finally {
                setCreating(false);
            }
        },
        [createDocument, history, entityRegistry],
    );

    if (!isContextBaseEnabled) {
        return null;
    }

    if (isCollapsed) {
        return null;
    }

    // Render documents tree (reads from DocumentTreeContext)
    return {
        type: NavBarMenuItemTypes.Group,
        key: 'context',
        title: '',
        renderTitle: () => (
            <ContextGroupHeader
                title="Context"
                onAddClick={() => handleCreateDocument()}
                onSearchClick={() => setShowSearchPopover(true)}
                isLoading={creating}
                searchPopoverContent={<SearchDocumentPopover onClose={() => setShowSearchPopover(false)} />}
                showSearchPopover={showSearchPopover}
                onSearchPopoverChange={(visible) => !visible && setShowSearchPopover(false)}
            />
        ),
        items: [
            {
                type: NavBarMenuItemTypes.Custom,
                key: 'documentTree',
                render: () => (
                    <DocumentTree onCreateChild={(parentUrn) => handleCreateDocument(parentUrn || undefined)} />
                ),
            },
        ],
    };
}
