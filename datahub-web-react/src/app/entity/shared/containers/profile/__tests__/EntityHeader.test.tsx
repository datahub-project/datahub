import { platformPrivileges } from '../../../../../../Mocks';
import { EntityType } from '../../../../../../types.generated';
import { getCanEditName } from '../header/EntityHeader';

describe('getCanEditName', () => {
    const entityDataWithManagePrivileges = { privileges: { canManageEntity: true, canEditProperties: true } };
    const entityDataWithoutManagePrivileges = { privileges: { canManageEntity: false, canEditProperties: false } };

    it('should return true for Terms if manageGlossaries privilege is true', () => {
        const canEditName = getCanEditName(
            EntityType.GlossaryTerm,
            entityDataWithoutManagePrivileges,
            true,
            platformPrivileges,
        );

        expect(canEditName).toBe(true);
    });

    it('should return false for Terms if manageGlossaries privilege and canManageEntity is false', () => {
        const privilegesWithoutGlossaries = { ...platformPrivileges, manageGlossaries: false };
        const canEditName = getCanEditName(
            EntityType.GlossaryTerm,
            entityDataWithoutManagePrivileges,
            true,
            privilegesWithoutGlossaries,
        );

        expect(canEditName).toBe(false);
    });

    it('should return true for Terms if manageGlossaries privilege is false but canManageEntity is true', () => {
        const privilegesWithoutGlossaries = { ...platformPrivileges, manageGlossaries: false };
        const canEditName = getCanEditName(
            EntityType.GlossaryTerm,
            entityDataWithManagePrivileges,
            true,
            privilegesWithoutGlossaries,
        );

        expect(canEditName).toBe(true);
    });

    it('should return true for Nodes if manageGlossaries privilege is true', () => {
        const canEditName = getCanEditName(
            EntityType.GlossaryNode,
            entityDataWithoutManagePrivileges,
            true,
            platformPrivileges,
        );

        expect(canEditName).toBe(true);
    });

    it('should return false for Nodes if manageGlossaries privilege and canManageEntity is false', () => {
        const privilegesWithoutGlossaries = { ...platformPrivileges, manageGlossaries: false };
        const canEditName = getCanEditName(
            EntityType.GlossaryNode,
            entityDataWithoutManagePrivileges,
            true,
            privilegesWithoutGlossaries,
        );

        expect(canEditName).toBe(false);
    });

    it('should return true for Nodes if manageGlossaries privilege is false but canManageEntity is true', () => {
        const privilegesWithoutGlossaries = { ...platformPrivileges, manageGlossaries: false };
        const canEditName = getCanEditName(
            EntityType.GlossaryNode,
            entityDataWithManagePrivileges,
            true,
            privilegesWithoutGlossaries,
        );

        expect(canEditName).toBe(true);
    });

    it('should return true for Domains if manageDomains privilege is true', () => {
        const canEditName = getCanEditName(
            EntityType.Domain,
            entityDataWithoutManagePrivileges,
            true,
            platformPrivileges,
        );

        expect(canEditName).toBe(true);
    });

    it('should return false for Domains if manageDomains privilege is false', () => {
        const privilegesWithoutDomains = { ...platformPrivileges, manageDomains: false };
        const canEditName = getCanEditName(
            EntityType.Domain,
            entityDataWithoutManagePrivileges,
            true,
            privilegesWithoutDomains,
        );

        expect(canEditName).toBe(false);
    });

    it('should return false for an unsupported entity', () => {
        const canEditName = getCanEditName(EntityType.Chart, entityDataWithManagePrivileges, true, platformPrivileges);

        expect(canEditName).toBe(false);
    });

    it('should return true for a dataset if canEditProperties is true', () => {
        const canEditName = getCanEditName(EntityType.Chart, entityDataWithManagePrivileges, true, platformPrivileges);

        expect(canEditName).toBe(false);
    });

    it('should return false for a dataset if canEditProperties is false', () => {
        const canEditName = getCanEditName(
            EntityType.Chart,
            entityDataWithoutManagePrivileges,
            true,
            platformPrivileges,
        );

        expect(canEditName).toBe(false);
    });

    it('should return false for a dataset if isEditableDatasetNameEnabled is false', () => {
        const canEditName = getCanEditName(EntityType.Chart, entityDataWithManagePrivileges, false, platformPrivileges);

        expect(canEditName).toBe(false);
    });
});
