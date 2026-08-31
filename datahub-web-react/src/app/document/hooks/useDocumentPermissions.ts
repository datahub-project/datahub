import { useMemo } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { useEntityData } from '@app/entity/shared/EntityContext';

import { Document, DocumentSourceType } from '@types';

interface DocumentPermissions {
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
 *
 * External documents (ingested from Confluence, Notion, etc.) treat state and type as
 * read-only because ingestion owns those fields and would overwrite any UI edits.
 */
export function useDocumentPermissions(_documentUrn?: string): DocumentPermissions {
    const { entityData } = useEntityData();
    const { platformPrivileges } = useUserContext();

    return useMemo(() => {
        const isExternal = (entityData as Document)?.info?.source?.sourceType === DocumentSourceType.External;

        // Platform-level privilege check
        const hasManageDocuments = platformPrivileges?.manageDocuments || false;

        // Entity-level privilege checks from document.privileges
        const canEditDescription = entityData?.privileges?.canEditDescription || false;
        const canManageEntity = entityData?.privileges?.canManageEntity || false;

        // Delete and move require either permissions for the entity or management at the platform level.
        const canDelete = canManageEntity || hasManageDocuments;
        const canMove = canManageEntity || hasManageDocuments;

        return {
            canCreate: hasManageDocuments,
            canEditContents: canEditDescription,
            canEditTitle: canEditDescription,
            // Ingestion owns state and type for external documents — UI edits would be overwritten.
            canEditState: isExternal ? false : canEditDescription,
            canEditType: isExternal ? false : canEditDescription,
            canDelete,
            canMove,
        };
    }, [entityData, platformPrivileges]);
}
