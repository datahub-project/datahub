import { useUserContext } from '@app/context/useUserContext';

/**
 * Whether the user can create terms and term groups. Mirrors the backend: creating needs Manage
 * Glossaries (anywhere) or permission to manage the parent group's children.
 *
 * @param canManageChildren the parent group's `privileges.canManageChildren`, which the backend
 *     already sets for Manage Glossaries holders; omit at the glossary root
 */
export default function useGlossaryCreatePrivileges(canManageChildren?: boolean | null) {
    const { platformPrivileges } = useUserContext();
    return { canCreate: canManageChildren ?? !!platformPrivileges?.manageGlossaries };
}
