import React from 'react';

import { EntityCapabilityType } from '@app/entity/Entity';
import DataProductsDropdown from '@app/entity/shared/components/styled/search/action/DataProductsDropdown';
import DeleteDropdown from '@app/entity/shared/components/styled/search/action/DeleteDropdown';
import DeprecationDropdown from '@app/entity/shared/components/styled/search/action/DeprecationDropdown';
import DomainDropdown from '@app/entity/shared/components/styled/search/action/DomainsDropdown';
import GlossaryTermDropdown from '@app/entity/shared/components/styled/search/action/GlossaryTermsDropdown';
import OwnersDropdown from '@app/entity/shared/components/styled/search/action/OwnersDropdown';
import TagsDropdown from '@app/entity/shared/components/styled/search/action/TagsDropdown';
import { SelectActionGroups } from '@app/entity/shared/components/styled/search/types';
import { EntityAndType } from '@app/entity/shared/types';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

/**
 * The set of action groups that are visible by default.
 *
 * Currently, only the change tags action is implemented.
 */
const DEFAULT_ACTION_GROUPS = [
    SelectActionGroups.CHANGE_TAGS,
    SelectActionGroups.CHANGE_GLOSSARY_TERMS,
    SelectActionGroups.CHANGE_DOMAINS,
    SelectActionGroups.CHANGE_OWNERS,
    SelectActionGroups.CHANGE_DEPRECATION,
    SelectActionGroups.DELETE,
    SelectActionGroups.CHANGE_DATA_PRODUCTS,
];

type Props = {
    selectedEntities: EntityAndType[];
    visibleActionGroups?: Set<SelectActionGroups>;
    refetch?: () => void;
};

/**
 * A component used for rendering a group of actions to take on a group of selected entities such
 * as changing owners, tags, domains, etc.
 */
export const SearchSelectActions = ({
    selectedEntities,
    visibleActionGroups = new Set(DEFAULT_ACTION_GROUPS),
    refetch,
}: Props) => {
    const entityRegistry = useEntityRegistry();

    /**
     * Extract the urns and entity types, which are used for a) qualifying actions
     * and b) executing actions.
     */
    const selectedEntityUrns = selectedEntities.map((entity) => entity.urn);
    const selectedEntityTypes = new Set(selectedEntities.map((entity) => entity.type));

    /**
     * Returns true if a specific capability is supported by ALL entities in a set.
     */
    const isEntityCapabilitySupported = (type: EntityCapabilityType, entityTypes: Set<EntityType>) => {
        return Array.from(entityTypes).every((entityType) =>
            entityRegistry.getSupportedEntityCapabilities(entityType).has(type),
        );
    };

    return (
        <>
            {visibleActionGroups.has(SelectActionGroups.CHANGE_OWNERS) && (
                <OwnersDropdown
                    urns={selectedEntityUrns}
                    disabled={
                        selectedEntityUrns.length === 0 ||
                        !isEntityCapabilitySupported(EntityCapabilityType.OWNERS, selectedEntityTypes)
                    }
                    refetch={refetch}
                />
            )}
            {visibleActionGroups.has(SelectActionGroups.CHANGE_GLOSSARY_TERMS) && (
                <GlossaryTermDropdown
                    urns={selectedEntityUrns}
                    disabled={
                        selectedEntityUrns.length === 0 ||
                        !isEntityCapabilitySupported(EntityCapabilityType.GLOSSARY_TERMS, selectedEntityTypes)
                    }
                    refetch={refetch}
                />
            )}
            {visibleActionGroups.has(SelectActionGroups.CHANGE_TAGS) && (
                <TagsDropdown
                    urns={selectedEntityUrns}
                    disabled={
                        selectedEntityUrns.length === 0 ||
                        !isEntityCapabilitySupported(EntityCapabilityType.TAGS, selectedEntityTypes)
                    }
                    refetch={refetch}
                />
            )}
            {visibleActionGroups.has(SelectActionGroups.CHANGE_DOMAINS) && (
                <DomainDropdown
                    urns={selectedEntityUrns}
                    disabled={
                        selectedEntityUrns.length === 0 ||
                        !isEntityCapabilitySupported(EntityCapabilityType.DOMAINS, selectedEntityTypes)
                    }
                    refetch={refetch}
                />
            )}
            {visibleActionGroups.has(SelectActionGroups.CHANGE_DEPRECATION) && (
                <DeprecationDropdown
                    urns={selectedEntityUrns}
                    disabled={
                        selectedEntityUrns.length === 0 ||
                        !isEntityCapabilitySupported(EntityCapabilityType.DEPRECATION, selectedEntityTypes)
                    }
                    refetch={refetch}
                />
            )}
            {visibleActionGroups.has(SelectActionGroups.DELETE) && (
                <DeleteDropdown
                    urns={selectedEntityUrns}
                    disabled={
                        selectedEntityUrns.length === 0 ||
                        !isEntityCapabilitySupported(EntityCapabilityType.SOFT_DELETE, selectedEntityTypes)
                    }
                    refetch={refetch}
                />
            )}
            {visibleActionGroups.has(SelectActionGroups.CHANGE_DATA_PRODUCTS) && (
                <DataProductsDropdown
                    urns={selectedEntityUrns}
                    disabled={
                        selectedEntityUrns.length === 0 ||
                        !isEntityCapabilitySupported(EntityCapabilityType.DATA_PRODUCTS, selectedEntityTypes)
                    }
                    refetch={refetch}
                />
            )}
        </>
    );
};
