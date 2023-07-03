import { platformPrivileges } from '../../../../../../Mocks';
import { EntityType } from '../../../../../../types.generated';
import { getCanEditName } from '../header/EntityHeader';

describe('getCanEditName', () => {
    const entityDataWithManagePrivileges = { privileges: { canManageEntity: true } };
    const entityDataWithoutManagePrivileges = { privileges: { canManageEntity: false } };

    it('should return true for Terms if manageGlossaries privilege is true', () => {
        const canEditName = getCanEditName(
            EntityType.GlossaryTerm,
            entityDataWithoutManagePrivileges,
            platformPrivileges,
        );

        expect(canEditName).toBe(true);
    });

    it('should return false for Terms if manageGlossaries privilege and canManageEntity is false', () => {
        const privilegesWithoutGlossaries = { ...platformPrivileges, manageGlossaries: false };
        const canEditName = getCanEditName(
            EntityType.GlossaryTerm,
            entityDataWithoutManagePrivileges,
            privilegesWithoutGlossaries,
        );

        expect(canEditName).toBe(false);
    });

    it('should return true for Terms if manageGlossaries privilege is false but canManageEntity is true', () => {
        const privilegesWithoutGlossaries = { ...platformPrivileges, manageGlossaries: false };
        const canEditName = getCanEditName(
            EntityType.GlossaryTerm,
            entityDataWithManagePrivileges,
            privilegesWithoutGlossaries,
        );

        expect(canEditName).toBe(true);
    });

    it('should return true for Nodes if manageGlossaries privilege is true', () => {
        const canEditName = getCanEditName(
            EntityType.GlossaryNode,
            entityDataWithoutManagePrivileges,
            platformPrivileges,
        );

        expect(canEditName).toBe(true);
    });

    it('should return false for Nodes if manageGlossaries privilege and canManageEntity is false', () => {
        const privilegesWithoutGlossaries = { ...platformPrivileges, manageGlossaries: false };
        const canEditName = getCanEditName(
            EntityType.GlossaryNode,
            entityDataWithoutManagePrivileges,
            privilegesWithoutGlossaries,
        );

        expect(canEditName).toBe(false);
    });

    it('should return true for Nodes if manageGlossaries privilege is false but canManageEntity is true', () => {
        const privilegesWithoutGlossaries = { ...platformPrivileges, manageGlossaries: false };
        const canEditName = getCanEditName(
            EntityType.GlossaryNode,
            entityDataWithManagePrivileges,
            privilegesWithoutGlossaries,
        );

        expect(canEditName).toBe(true);
    });

    it('should return true for Domains if manageDomains privilege is true', () => {
        const canEditName = getCanEditName(EntityType.Domain, entityDataWithoutManagePrivileges, platformPrivileges);

        expect(canEditName).toBe(true);
    });

    it('should return false for Domains if manageDomains privilege is false', () => {
        const privilegesWithoutDomains = { ...platformPrivileges, manageDomains: false };
        const canEditName = getCanEditName(
            EntityType.Domain,
            entityDataWithoutManagePrivileges,
            privilegesWithoutDomains,
        );

        expect(canEditName).toBe(false);
    });

    it('should return false for an unsupported entity', () => {
        const canEditName = getCanEditName(EntityType.Chart, entityDataWithManagePrivileges, platformPrivileges);

        expect(canEditName).toBe(false);
    });
});
