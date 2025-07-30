import React from 'react';
import styled from 'styled-components/macro';

import { useUserContext } from '@app/context/useUserContext';
import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import EntityDropdown, { EntityMenuItems } from '@app/entity/shared/EntityDropdown/EntityDropdown';
import ExternalUrlButton from '@app/entity/shared/ExternalUrlButton';
import { DeprecationPill } from '@app/entity/shared/components/styled/DeprecationPill';
import EntityCount from '@app/entity/shared/containers/profile/header/EntityCount';
import EntityHeaderLoadingSection from '@app/entity/shared/containers/profile/header/EntityHeaderLoadingSection';
import { EntityHealth } from '@app/entity/shared/containers/profile/header/EntityHealth';
import EntityName from '@app/entity/shared/containers/profile/header/EntityName';
import PlatformContent from '@app/entity/shared/containers/profile/header/PlatformContent';
import StructuredPropertyBadge from '@app/entity/shared/containers/profile/header/StructuredPropertyBadge';
import EntityActions, { EntityActionItem } from '@app/entity/shared/entity/EntityActions';
import { EntitySubHeaderSection, GenericEntityProperties } from '@app/entity/shared/types';
import { getPlatformName } from '@app/entity/shared/utils';
import ShareButton from '@app/shared/share/ShareButton';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useIsEditableDatasetNameEnabled } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, PlatformPrivileges } from '@types';

const TitleWrapper = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
    gap: 8px;

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
    isEditableDatasetNameEnabled: boolean,
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
        case EntityType.BusinessAttribute:
            return privileges?.manageBusinessAttributes;
        case EntityType.Dataset:
            return isEditableDatasetNameEnabled && entityData?.privileges?.canEditProperties;
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
    const { urn, entityType, entityData, loading } = useEntityData();
    const refetch = useRefetch();
    const me = useUserContext();
    const platformName = getPlatformName(entityData);
    const externalUrl = entityData?.externalUrl || undefined;
    const entityCount = entityData?.entityCount;

    const entityName = entityData?.name;
    const subType = capitalizeFirstLetterOnly(entityData?.subTypes?.typeNames?.[0]) || undefined;

    const isEditableDatasetNameEnabled = useIsEditableDatasetNameEnabled();
    const canEditName =
        isNameEditable &&
        getCanEditName(
            entityType,
            entityData,
            isEditableDatasetNameEnabled,
            me?.platformPrivileges as PlatformPrivileges,
        );
    const entityRegistry = useEntityRegistry();

    return (
        <>
            <HeaderContainer data-testid="entity-header-test-id">
                <MainHeaderContent>
                    {(loading && <EntityHeaderLoadingSection />) || (
                        <>
                            <PlatformContent />
                            <TitleWrapper>
                                <EntityName isNameEditable={canEditName || false} />
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
                                <StructuredPropertyBadge structuredProperties={entityData?.structuredProperties} />
                            </TitleWrapper>
                            <EntityCount
                                entityCount={entityCount}
                                displayAssetsText={entityType === EntityType.DataProduct}
                            />
                        </>
                    )}
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
