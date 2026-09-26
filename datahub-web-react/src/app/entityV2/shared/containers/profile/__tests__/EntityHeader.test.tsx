import { getCanEditEntityProperties } from '@app/entityV2/shared/containers/profile/header/EntityHeader';
import { platformPrivileges } from '@src/Mocks';

import { EntityType } from '@types';

describe('getCanEditEntityProperties', () => {
    const entityDataWithManagePrivileges = { privileges: { canManageEntity: true } };
    const entityDataWithoutManagePrivileges = { privileges: { canManageEntity: false } };

    it('should return true for Terms if manageGlossaries privilege is true', () => {
        const result = getCanEditEntityProperties(
            EntityType.GlossaryTerm,
            entityDataWithoutManagePrivileges,
            platformPrivileges,
        );

        expect(result).toBe(true);
    });

    it('should return false for Terms if manageGlossaries privilege and canManageEntity is false', () => {
        const privilegesWithoutGlossaries = { ...platformPrivileges, manageGlossaries: false };
        const result = getCanEditEntityProperties(
            EntityType.GlossaryTerm,
            entityDataWithoutManagePrivileges,
            privilegesWithoutGlossaries,
        );

        expect(result).toBe(false);
    });

    it('should return true for Terms if manageGlossaries privilege is false but canManageEntity is true', () => {
        const privilegesWithoutGlossaries = { ...platformPrivileges, manageGlossaries: false };
        const result = getCanEditEntityProperties(
            EntityType.GlossaryTerm,
            entityDataWithManagePrivileges,
            privilegesWithoutGlossaries,
        );

        expect(result).toBe(true);
    });

    it('should return true for Nodes if manageGlossaries privilege is true', () => {
        const result = getCanEditEntityProperties(
            EntityType.GlossaryNode,
            entityDataWithoutManagePrivileges,
            platformPrivileges,
        );

        expect(result).toBe(true);
    });

    it('should return false for Nodes if manageGlossaries privilege and canManageEntity is false', () => {
        const privilegesWithoutGlossaries = { ...platformPrivileges, manageGlossaries: false };
        const result = getCanEditEntityProperties(
            EntityType.GlossaryNode,
            entityDataWithoutManagePrivileges,
            privilegesWithoutGlossaries,
        );

        expect(result).toBe(false);
    });

    it('should return true for Nodes if manageGlossaries privilege is false but canManageEntity is true', () => {
        const privilegesWithoutGlossaries = { ...platformPrivileges, manageGlossaries: false };
        const result = getCanEditEntityProperties(
            EntityType.GlossaryNode,
            entityDataWithManagePrivileges,
            privilegesWithoutGlossaries,
        );

        expect(result).toBe(true);
    });

    it('should return true for Domains if manageDomains privilege is true', () => {
        const result = getCanEditEntityProperties(
            EntityType.Domain,
            entityDataWithoutManagePrivileges,
            platformPrivileges,
        );

        expect(result).toBe(true);
    });

    it('should return false for Domains if manageDomains privilege is false', () => {
        const privilegesWithoutDomains = { ...platformPrivileges, manageDomains: false };
        const result = getCanEditEntityProperties(
            EntityType.Domain,
            entityDataWithoutManagePrivileges,
            privilegesWithoutDomains,
        );

        expect(result).toBe(false);
    });

    it('should return false for an unsupported entity', () => {
        const result = getCanEditEntityProperties(EntityType.Chart, entityDataWithManagePrivileges, platformPrivileges);

        expect(result).toBe(false);
    });
});
