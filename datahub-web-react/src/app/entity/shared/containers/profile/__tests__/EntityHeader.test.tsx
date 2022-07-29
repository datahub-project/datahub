import { platformPrivileges } from '../../../../../../Mocks';
import { EntityType } from '../../../../../../types.generated';
import { getCanEditName } from '../header/EntityHeader';

describe('getCanEditName', () => {
    it('should return true for Terms if manageGlossaries privilege is true', () => {
        const canEditName = getCanEditName(EntityType.GlossaryTerm, platformPrivileges);

        expect(canEditName).toBe(true);
    });

    it('should return false for Terms if manageGlossaries privilege is false', () => {
        const privilegesWithoutGlossaries = { ...platformPrivileges, manageGlossaries: false };
        const canEditName = getCanEditName(EntityType.GlossaryTerm, privilegesWithoutGlossaries);

        expect(canEditName).toBe(false);
    });

    it('should return true for Nodes if manageGlossaries privilege is true', () => {
        const canEditName = getCanEditName(EntityType.GlossaryNode, platformPrivileges);

        expect(canEditName).toBe(true);
    });

    it('should return false for Nodes if manageGlossaries privilege is false', () => {
        const privilegesWithoutGlossaries = { ...platformPrivileges, manageGlossaries: false };
        const canEditName = getCanEditName(EntityType.GlossaryNode, privilegesWithoutGlossaries);

        expect(canEditName).toBe(false);
    });

    it('should return true for Domains if manageDomains privilege is true', () => {
        const canEditName = getCanEditName(EntityType.Domain, platformPrivileges);

        expect(canEditName).toBe(true);
    });

    it('should return false for Domains if manageDomains privilege is false', () => {
        const privilegesWithoutDomains = { ...platformPrivileges, manageDomains: false };
        const canEditName = getCanEditName(EntityType.Domain, privilegesWithoutDomains);

        expect(canEditName).toBe(false);
    });

    it('should return false for an unsupported entity', () => {
        const canEditName = getCanEditName(EntityType.Chart, platformPrivileges);

        expect(canEditName).toBe(false);
    });
});
