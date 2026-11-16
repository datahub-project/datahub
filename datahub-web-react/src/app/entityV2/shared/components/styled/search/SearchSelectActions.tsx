import React from 'react';
import { useTranslation } from 'react-i18next';

import { EntityCapabilityType } from '@app/entityV2/Entity';
import DataProductsDropdown from '@app/entityV2/shared/components/styled/search/action/DataProductsDropdown';
import DeleteDropdown from '@app/entityV2/shared/components/styled/search/action/DeleteDropdown';
import DeprecationDropdown from '@app/entityV2/shared/components/styled/search/action/DeprecationDropdown';
import DomainDropdown from '@app/entityV2/shared/components/styled/search/action/DomainsDropdown';
import GlossaryTermDropdown from '@app/entityV2/shared/components/styled/search/action/GlossaryTermsDropdown';
import OwnersDropdown from '@app/entityV2/shared/components/styled/search/action/OwnersDropdown';
import TagsDropdown from '@app/entityV2/shared/components/styled/search/action/TagsDropdown';
import { SelectActionGroups } from '@app/entityV2/shared/components/styled/search/types';
import { EntityAndType } from '@app/entityV2/shared/types';
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
    const { t } = useTranslation('entity.shared.components');
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

    const getDisabledReason = (type: EntityCapabilityType): string | undefined => {
        if (selectedEntityUrns.length === 0) return t('searchActions.noSelection');
        if (!isEntityCapabilitySupported(type, selectedEntityTypes)) return t('searchActions.notSupported');
        return undefined;
    };

    return (
        <>
            {visibleActionGroups.has(SelectActionGroups.CHANGE_OWNERS) && (
                <OwnersDropdown
                    urns={selectedEntityUrns}
                    disabledReason={getDisabledReason(EntityCapabilityType.OWNERS)}
                    refetch={refetch}
                />
            )}
            {visibleActionGroups.has(SelectActionGroups.CHANGE_GLOSSARY_TERMS) && (
                <GlossaryTermDropdown
                    urns={selectedEntityUrns}
                    disabledReason={getDisabledReason(EntityCapabilityType.GLOSSARY_TERMS)}
                    refetch={refetch}
                />
            )}
            {visibleActionGroups.has(SelectActionGroups.CHANGE_TAGS) && (
                <TagsDropdown
                    urns={selectedEntityUrns}
                    disabledReason={getDisabledReason(EntityCapabilityType.TAGS)}
                    refetch={refetch}
                />
            )}
            {visibleActionGroups.has(SelectActionGroups.CHANGE_DOMAINS) && (
                <DomainDropdown
                    urns={selectedEntityUrns}
                    disabledReason={getDisabledReason(EntityCapabilityType.DOMAINS)}
                    refetch={refetch}
                />
            )}
            {visibleActionGroups.has(SelectActionGroups.CHANGE_DEPRECATION) && (
                <DeprecationDropdown
                    urns={selectedEntityUrns}
                    disabledReason={getDisabledReason(EntityCapabilityType.DEPRECATION)}
                    refetch={refetch}
                />
            )}
            {visibleActionGroups.has(SelectActionGroups.DELETE) && (
                <DeleteDropdown
                    urns={selectedEntityUrns}
                    disabledReason={getDisabledReason(EntityCapabilityType.SOFT_DELETE)}
                    refetch={refetch}
                />
            )}
            {visibleActionGroups.has(SelectActionGroups.CHANGE_DATA_PRODUCTS) && (
                <DataProductsDropdown
                    urns={selectedEntityUrns}
                    disabledReason={getDisabledReason(EntityCapabilityType.DATA_PRODUCTS)}
                    refetch={refetch}
                />
            )}
        </>
    );
};
