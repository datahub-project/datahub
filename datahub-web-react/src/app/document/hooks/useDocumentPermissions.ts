import { useMemo } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { useEntityData } from '@app/entity/shared/EntityContext';

import { Document } from '@types';

export interface DocumentPermissions {
    canCreate: boolean;
    canEditContents: boolean;
    canEditTitle: boolean;
    canEditState: boolean;
    canEditType: boolean;
    canDelete: boolean;
    canMove: boolean;
}

/**
 * Hook to determine user permissions for a document.
 *
 * Permission Rules:
 * - Document Contents, Title, State, Type: Requires EDIT_ENTITY_DOCS privilege for asset
 * - Owners, Tags, Terms, Domain, Data Product: Requires the respective EDIT_X privilege
 * - Create/Delete/Move: Requires EDIT_ENTITY or MANAGE_DOCUMENTS privilege.
 */
export function useDocumentPermissions(_documentUrn?: string): DocumentPermissions {
    const { entityData } = useEntityData();
    const { platformPrivileges } = useUserContext();
    const document = entityData as Document;

    return useMemo(() => {
        // Platform-level privilege check
        const hasManageDocuments = platformPrivileges?.manageDocuments || false;

        // Entity-level privilege checks from document.privileges
        const canEditDescription = document?.privileges?.canEditDescription || false;
        const canManageEntity = document?.privileges?.canManageEntity || false;

        // Delete and move require either permissions for the entity or management at the platform level.
        const canDelete = canManageEntity || hasManageDocuments;
        const canMove = canManageEntity || hasManageDocuments;

        return {
            canCreate: hasManageDocuments,
            // All the same here.
            canEditContents: canEditDescription,
            canEditTitle: canEditDescription,
            canEditState: canEditDescription,
            canEditType: canEditDescription,
            canDelete,
            canMove,
        };
    }, [document, platformPrivileges]);
}
