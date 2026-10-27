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
 * - Create: Requires CREATE_ENTITY or EDIT_ENTITY for Documents, or platform MANAGE_DOCUMENTS.
 * - Delete: Requires DELETE_ENTITY on the document or platform MANAGE_DOCUMENTS.
 * - Move: Requires EDIT_ENTITY on the document or platform MANAGE_DOCUMENTS.
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
        const canCreateDocuments = platformPrivileges?.createDocuments || false;

        // Entity-level privilege checks from document.privileges
        const canEditDescription = entityData?.privileges?.canEditDescription || false;
        const canManageEntity = entityData?.privileges?.canManageEntity || false;
        const canDeleteEntity = entityData?.privileges?.canDeleteEntity || false;

        // Delete and move require either permissions for the entity or management at the platform level.
        const canDelete = canDeleteEntity || hasManageDocuments;
        const canMove = canManageEntity || hasManageDocuments;

        // Edit rights require entity data to be loaded. Once loaded, either the entity-level
        // canEditDescription privilege OR the platform-level manageDocuments privilege grants access.
        const canEditContents = !!entityData && (canEditDescription || hasManageDocuments);
        const canEditTitle = !!entityData && (canEditDescription || hasManageDocuments);
        // Ingestion owns state and type for external documents — UI edits would be overwritten.
        const canEditState = isExternal ? false : !!entityData && (canEditDescription || hasManageDocuments);

        return {
            canCreate: canCreateDocuments,
            canEditContents,
            canEditTitle,
            canEditState,
            canEditType: isExternal ? false : !!entityData && (canEditDescription || hasManageDocuments),
            canDelete,
            canMove,
        };
    }, [entityData, platformPrivileges]);
}
