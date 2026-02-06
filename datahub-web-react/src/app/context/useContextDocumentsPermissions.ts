import { useMemo } from 'react';

import { useUserContext } from '@app/context/useUserContext';

/**
 * Permissions for the Context Documents section (sidebar and landing page).
 * These are platform-level permissions, not entity-level.
 */
export interface ContextDocumentsPermissions {
    /** Whether the user can create new documents */
    canCreate: boolean;
    /** Whether the user can manage (delete/move) any documents */
    canManage: boolean;
}

/**
 * Hook to determine user permissions for the Context Documents section.
 *
 * This provides platform-level permissions for the document tree sidebar
 * and landing page. For entity-level permissions on a specific document,
 * use `useDocumentPermissions` instead.
 *
 * Permission Rules:
 * - Create: Requires MANAGE_DOCUMENTS platform privilege
 * - Manage (delete/move): Requires MANAGE_DOCUMENTS platform privilege
 *
 * @returns ContextDocumentsPermissions object with permission flags
 */
export function useContextDocumentsPermissions(): ContextDocumentsPermissions {
    const { platformPrivileges, loaded } = useUserContext();

    return useMemo(() => {
        // Wait for user context to load
        if (!loaded) {
            return {
                canCreate: false,
                canManage: false,
            };
        }

        const hasManageDocuments = platformPrivileges?.manageDocuments || false;

        return {
            canCreate: hasManageDocuments,
            canManage: hasManageDocuments,
        };
    }, [platformPrivileges, loaded]);
}
