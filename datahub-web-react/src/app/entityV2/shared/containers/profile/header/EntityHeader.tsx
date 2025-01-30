import React from 'react';
import styled from 'styled-components/macro';
import { EntityMenuItems } from '../../../EntityDropdown/EntityMenuActions';
import { DisplayProperties, EntityType, PlatformPrivileges } from '../../../../../../types.generated';
import { EntityActionItem } from '../../../entity/EntityActions';
import { useUserContext } from '../../../../../context/useUserContext';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { DefaultEntityHeader } from './DefaultEntityHeader';
import { useEntityData, useRefetch } from '../../../../../entity/shared/EntityContext';
import { EntitySubHeaderSection, GenericEntityProperties } from '../../../../../entity/shared/types';

const Container = styled.div``;

export function getCanEditName(
    entityType: EntityType,
    entityData: GenericEntityProperties | null,
    privileges?: PlatformPrivileges,
) {
    switch (entityType) {
        case EntityType.GlossaryTerm:
        case EntityType.GlossaryNode:
            return privileges?.manageGlossaries || !!entityData?.privileges?.canManageEntity;
        case EntityType.Domain:
            return privileges?.manageDomains;
        case EntityType.DataProduct:
            return true; // TODO: add permissions for data products
        default:
            return false;
    }
}

type Props = {
    headerDropdownItems?: Set<EntityMenuItems>;
    headerActionItems?: Set<EntityActionItem>;
    isNameEditable?: boolean;
    isIconEditable?: boolean;
    isColorEditable?: boolean;
    displayProperties?: DisplayProperties;
    subHeader?: EntitySubHeaderSection;
};

export const EntityHeader = ({
    headerDropdownItems,
    headerActionItems,
    isNameEditable,
    isIconEditable,
    isColorEditable,
    displayProperties,
    subHeader,
}: Props) => {
    const { urn, entityType, entityData, loading } = useEntityData();
    const refetch = useRefetch();
    const me = useUserContext();
    const entityRegistry = useEntityRegistry();

    const entityUrl = entityRegistry.getEntityUrl(entityType, urn);
    const showEditName =
        isNameEditable && getCanEditName(entityType, entityData, me?.platformPrivileges as PlatformPrivileges);

    return (
        <Container data-testid="entity-header-test-id">
            <DefaultEntityHeader
                entityType={entityType}
                urn={urn}
                entityUrl={entityUrl}
                loading={loading}
                entityData={entityData}
                refetch={refetch}
                showEditName={showEditName}
                isColorEditable={isColorEditable}
                isIconEditable={isIconEditable}
                displayProperties={displayProperties}
                headerActionItems={headerActionItems}
                headerDropdownItems={headerDropdownItems}
                subHeader={subHeader}
            />
        </Container>
    );
};
