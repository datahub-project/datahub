import React from 'react';
import styled from 'styled-components/macro';
import { useEntityData, useRefetch } from '../../../EntityContext';
import EntityDropdown, { EntityMenuItems } from '../../../EntityDropdown/EntityDropdown';
import PlatformContent from './PlatformContent';
import { getPlatformName } from '../../../utils';
import { EntityType, PlatformPrivileges } from '../../../../../../types.generated';
import EntityCount from './EntityCount';
import { EntityHealth } from './EntityHealth';
import EntityName from './EntityName';
import { DeprecationPill } from '../../../components/styled/DeprecationPill';
import { EntitySubHeaderSection, GenericEntityProperties } from '../../../types';
import EntityActions, { EntityActionItem } from '../../../entity/EntityActions';
import ExternalUrlButton from '../../../ExternalUrlButton';
import ShareButton from '../../../../../shared/share/ShareButton';
import { capitalizeFirstLetterOnly } from '../../../../../shared/textUtil';
import { useUserContext } from '../../../../../context/useUserContext';
import { useEntityRegistry } from '../../../../../useEntityRegistry';

const TitleWrapper = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;

    .ant-typography-edit-content {
        padding-top: 7px;
        margin-left: 15px;
    }
`;

const HeaderContainer = styled.div`
    display: flex;
    flex-direction: row;
    align-items: space-between;
    margin-bottom: 4px;
`;

const MainHeaderContent = styled.div`
    flex: 1;
    width: 70%;

    .entityCount {
        margin: 5px 0 -4px 0;
    }
`;

const SideHeaderContent = styled.div`
    display: flex;
    flex-direction: column;
`;

const TopButtonsWrapper = styled.div`
    display: flex;
    justify-content: flex-end;
    margin-bottom: 8px;
`;

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
    subHeader?: EntitySubHeaderSection;
};

export const EntityHeader = ({ headerDropdownItems, headerActionItems, isNameEditable, subHeader }: Props) => {
    const { urn, entityType, entityData } = useEntityData();
    const refetch = useRefetch();
    const me = useUserContext();
    const platformName = getPlatformName(entityData);
    const externalUrl = entityData?.externalUrl || undefined;
    const entityCount = entityData?.entityCount;

    const entityName = entityData?.name;
    const subType = capitalizeFirstLetterOnly(entityData?.subTypes?.typeNames?.[0]) || undefined;

    const canEditName =
        isNameEditable && getCanEditName(entityType, entityData, me?.platformPrivileges as PlatformPrivileges);
    const entityRegistry = useEntityRegistry();

    return (
        <>
            <HeaderContainer data-testid="entity-header-test-id">
                <MainHeaderContent>
                    <PlatformContent />
                    <TitleWrapper>
                        <EntityName isNameEditable={canEditName} />
                        {entityData?.deprecation?.deprecated && (
                            <DeprecationPill
                                urn={urn}
                                deprecation={entityData?.deprecation}
                                showUndeprecate
                                refetch={refetch}
                            />
                        )}
                        {entityData?.health && (
                            <EntityHealth
                                health={entityData.health}
                                baseUrl={entityRegistry.getEntityUrl(entityType, urn)}
                            />
                        )}
                    </TitleWrapper>
                    <EntityCount entityCount={entityCount} displayAssetsText={entityType === EntityType.DataProduct} />
                </MainHeaderContent>
                <SideHeaderContent>
                    <TopButtonsWrapper>
                        {externalUrl && (
                            <ExternalUrlButton
                                externalUrl={externalUrl}
                                entityUrn={urn}
                                platformName={platformName}
                                entityType={entityType}
                            />
                        )}
                        {headerActionItems && (
                            <EntityActions urn={urn} actionItems={headerActionItems} refetchForEntity={refetch} />
                        )}
                        <ShareButton entityType={entityType} subType={subType} urn={urn} name={entityName} />
                        {headerDropdownItems && (
                            <EntityDropdown
                                urn={urn}
                                entityType={entityType}
                                entityData={entityData}
                                menuItems={headerDropdownItems}
                                refetchForEntity={refetch}
                            />
                        )}
                    </TopButtonsWrapper>
                </SideHeaderContent>
            </HeaderContainer>
            {subHeader && <subHeader.component />}
        </>
    );
};
