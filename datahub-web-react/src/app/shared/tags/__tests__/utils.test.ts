import { EntityType } from '../../../../types.generated';
import { ENTITY_TYPES_WITH_PROPOSALS, shouldShowProposeButton } from '../utils/proposalUtils';

describe('utils', () => {
    it('should return true for entities with proposals', () => {
        expect(shouldShowProposeButton(EntityType.Dataset)).toBe(true);

        ENTITY_TYPES_WITH_PROPOSALS.forEach((entityType) => {
            expect(shouldShowProposeButton(entityType)).toBe(true);
        });
    });

    it('should return false for an entity type that does not have proposals', () => {
        expect(shouldShowProposeButton(EntityType.Assertion)).toBe(false);
        expect(shouldShowProposeButton(EntityType.Tag)).toBe(false);
    });
});
