import React, { useState } from 'react';
import { ArrowRightOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import styled from 'styled-components/macro';
import { capitalizeFirstLetterOnly } from '../../../../../shared/textUtil';
import { useEntityData, useRefetch } from '../../../EntityContext';
import analytics, { EventType, EntityActionType } from '../../../../../analytics';
import { EntityHealthStatus } from './EntityHealthStatus';
import EntityDropdown, { EntityMenuItems } from '../../../EntityDropdown/EntityDropdown';
import PlatformContent from './PlatformContent';
import { getPlatformName } from '../../../utils';
import { useGetAuthenticatedUser } from '../../../../../useGetAuthenticatedUser';
import { EntityType, PlatformPrivileges } from '../../../../../../types.generated';
import EntityCount from './EntityCount';
import EntityName from './EntityName';
import CopyUrn from '../../../../../shared/CopyUrn';
import { DeprecationPill } from '../../../components/styled/DeprecationPill';
import CompactContext from '../../../../../shared/CompactContext';

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
    width: 85%;

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

const ExternalUrlContainer = styled.span`
    font-size: 14px;
`;

const ExternalUrlButton = styled(Button)`
    > :hover {
        text-decoration: underline;
    }
    padding-left: 12px;
    padding-right: 12px;
`;

export function getCanEditName(entityType: EntityType, privileges?: PlatformPrivileges) {
    switch (entityType) {
        case EntityType.GlossaryTerm:
        case EntityType.GlossaryNode:
            return privileges?.manageGlossaries;
        case EntityType.Domain:
            return privileges?.manageDomains;
        default:
            return false;
    }
}

type Props = {
    refreshBrowser?: () => void;
    headerDropdownItems?: Set<EntityMenuItems>;
    isNameEditable?: boolean;
};

export const EntityHeader = ({ refreshBrowser, headerDropdownItems, isNameEditable }: Props) => {
    const { urn, entityType, entityData } = useEntityData();
    const refetch = useRefetch();
    const me = useGetAuthenticatedUser();
    const [copiedUrn, setCopiedUrn] = useState(false);
    const basePlatformName = getPlatformName(entityData);
    const platformName = capitalizeFirstLetterOnly(basePlatformName);
    const externalUrl = entityData?.externalUrl || undefined;
    const entityCount = entityData?.entityCount;
    const isCompact = React.useContext(CompactContext);

    const sendAnalytics = () => {
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.ClickExternalUrl,
            entityType,
            entityUrn: urn,
        });
    };

    const canEditName = isNameEditable && getCanEditName(entityType, me?.platformPrivileges as PlatformPrivileges);

    return (
        <HeaderContainer data-testid="entity-header-test-id">
            <MainHeaderContent>
                <PlatformContent />
                <TitleWrapper>
                    <EntityName isNameEditable={canEditName} />
                    {entityData?.deprecation?.deprecated && (
                        <DeprecationPill deprecation={entityData?.deprecation} preview={isCompact} />
                    )}
                    {entityData?.health?.map((health) => (
                        <EntityHealthStatus
                            type={health.type}
                            status={health.status}
                            message={health.message || undefined}
                        />
                    ))}
                </TitleWrapper>
                <EntityCount entityCount={entityCount} />
            </MainHeaderContent>
            <SideHeaderContent>
                <TopButtonsWrapper>
                    {externalUrl && (
                        <ExternalUrlContainer>
                            <ExternalUrlButton type="link" href={externalUrl} target="_blank" onClick={sendAnalytics}>
                                View in {platformName} <ArrowRightOutlined style={{ fontSize: 12 }} />
                            </ExternalUrlButton>
                        </ExternalUrlContainer>
                    )}
                    <CopyUrn urn={urn} isActive={copiedUrn} onClick={() => setCopiedUrn(true)} />
                    {headerDropdownItems && (
                        <EntityDropdown
                            urn={urn}
                            entityType={entityType}
                            entityData={entityData}
                            menuItems={headerDropdownItems}
                            refetchForEntity={refetch}
                            refreshBrowser={refreshBrowser}
                            platformPrivileges={me?.platformPrivileges as PlatformPrivileges}
                        />
                    )}
                </TopButtonsWrapper>
            </SideHeaderContent>
        </HeaderContainer>
    );
};
