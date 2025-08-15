import { uniqBy } from 'lodash';

import { EntityRegistry } from '@src/entityRegistryContext';

import { GetOwnedGroupsQuery } from '@graphql/group.generated';
import { CorpGroup, EntityRelationshipsResult, EntityType } from '@types';

export const convertGroupRelationshipToOption = (group: CorpGroup, entityRegistry: EntityRegistry) => {
    return {
        label: entityRegistry.getDisplayName(EntityType.CorpGroup, group),
        value: group?.urn,
    };
};

export const getGroupOptions = (
    relationships: EntityRelationshipsResult['relationships'] | undefined,
    ownedGroups: NonNullable<GetOwnedGroupsQuery['search']>['searchResults'] | undefined,
    entityRegistry: EntityRegistry,
) => {
    const memberGroupOptions =
        relationships
            ?.filter((relationship) => !!relationship.entity)
            .map((relationship) =>
                convertGroupRelationshipToOption(relationship.entity as CorpGroup, entityRegistry),
            ) || [];

    const ownedGroupOptions =
        ownedGroups?.map((group) => convertGroupRelationshipToOption(group.entity as CorpGroup, entityRegistry)) || [];

    return uniqBy([...memberGroupOptions, ...ownedGroupOptions], 'value');
};
