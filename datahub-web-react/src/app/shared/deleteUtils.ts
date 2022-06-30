import { useDeleteAssertionMutation } from '../../graphql/assertion.generated';
import { useDeleteDomainMutation } from '../../graphql/domain.generated';
import { useDeleteGlossaryEntityMutation } from '../../graphql/glossary.generated';
import { useRemoveGroupMutation } from '../../graphql/group.generated';
import { useDeleteTagMutation } from '../../graphql/tag.generated';
import { useRemoveUserMutation } from '../../graphql/user.generated';
import { EntityType } from '../../types.generated';

/**
 * Returns a relative redirect path which is used after an Entity has been deleted from it's profile page.
 *
 * @param type the entity type being deleted
 */
export const getEntityProfileDeleteRedirectPath = (type: EntityType) => {
    switch (type) {
        case EntityType.CorpGroup:
        case EntityType.CorpUser:
        case EntityType.Domain:
        case EntityType.Tag:
            // Return Home.
            return '/';
        case EntityType.GlossaryNode:
        case EntityType.GlossaryTerm:
            // Return to glossary page.
            return '/glossary';
        default:
            return () => undefined;
    }
};

/**
 * Returns a mutation hook for deleting an entity of a particular type.
 *
 * TODO: Push this back into the entity registry.
 *
 * @param type the entity type being deleted
 */
export const getDeleteEntityMutation = (type: EntityType) => {
    switch (type) {
        case EntityType.CorpGroup:
            return useRemoveGroupMutation;
        case EntityType.CorpUser:
            return useRemoveUserMutation;
        case EntityType.Assertion:
            return useDeleteAssertionMutation;
        case EntityType.Domain:
            return useDeleteDomainMutation;
        case EntityType.Tag:
            return useDeleteTagMutation;
        case EntityType.GlossaryNode:
        case EntityType.GlossaryTerm:
            return useDeleteGlossaryEntityMutation;
        default:
            return () => undefined;
    }
};
