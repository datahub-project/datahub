import { GenericEntityProperties } from '@app/entity/shared/types';
import { PageRoutes } from '@conf/Global';

import { useDeleteApplicationMutation } from '@graphql/application.generated';
import { useDeleteAssertionMutation } from '@graphql/assertion.generated';
import { useDeleteBusinessAttributeMutation } from '@graphql/businessAttribute.generated';
import { useDeleteDataProductMutation } from '@graphql/dataProduct.generated';
import { useDeleteDomainMutation } from '@graphql/domain.generated';
import { useDeleteGlossaryEntityMutation } from '@graphql/glossary.generated';
import { useRemoveGroupMutation } from '@graphql/group.generated';
import { useBatchUpdateSoftDeletedMutation } from '@graphql/mutations.generated';
import { useDeleteTagMutation } from '@graphql/tag.generated';
import { useRemoveUserMutation } from '@graphql/user.generated';
import { EntityType } from '@types';

/**
 * Returns a relative redirect path which is used after an Entity has been deleted from it's profile page.
 *
 * @param type the entity type being deleted
 */
export const getEntityProfileDeleteRedirectPath = (type: EntityType, entityData: GenericEntityProperties | null) => {
    const domain = entityData?.domain?.domain;
    switch (type) {
        case EntityType.CorpGroup:
        case EntityType.CorpUser:
        case EntityType.Application:
        case EntityType.Tag:
        case EntityType.Dataset:
            // Return Home. A dataset is only deletable from its profile when it's a logical model;
            // sending the user Home avoids leaving them on the now-deleted entity page.
            return '/';
        case EntityType.Domain:
            return `${PageRoutes.DOMAINS}`;
        case EntityType.GlossaryNode:
        case EntityType.GlossaryTerm:
            // Return to glossary page.
            return '/glossary';
        case EntityType.DataProduct:
            // Return to Data Products tab of the domain it was part of
            if (domain) {
                return `/domain/${domain.urn}/Data Products`;
            }
            return '/';
        case EntityType.BusinessAttribute:
            return `${PageRoutes.BUSINESS_ATTRIBUTE}`;
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
/**
 * Adapter exposing the generic soft-delete (Status.removed) with the `{ variables: { urn } }`
 * shape that the per-entity delete mutations use. Used for entities without a dedicated delete
 * mutation — e.g. datasets / logical models.
 */
const useSoftDeleteByUrnMutation = () => {
    const [batchUpdateSoftDeleted, result] = useBatchUpdateSoftDeletedMutation();
    const softDelete = (options: { variables: { urn: string } }) =>
        batchUpdateSoftDeleted({ variables: { input: { urns: [options.variables.urn], deleted: true } } });
    return [softDelete, result] as const;
};

export const getDeleteEntityMutation = (type: EntityType) => {
    switch (type) {
        case EntityType.Dataset:
            return useSoftDeleteByUrnMutation;
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
        case EntityType.DataProduct:
            return useDeleteDataProductMutation;
        case EntityType.BusinessAttribute:
            return useDeleteBusinessAttributeMutation;
        case EntityType.Application:
            return useDeleteApplicationMutation;
        default:
            return () => undefined;
    }
};
