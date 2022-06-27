import React, { useState } from 'react';
import { InfoCircleOutlined, RightOutlined } from '@ant-design/icons';
import { Typography, Button, Tooltip, Popover } from 'antd';
import styled from 'styled-components/macro';
import moment from 'moment';
import { capitalizeFirstLetterOnly } from '../../../../../shared/textUtil';
import { ANTD_GRAY } from '../../../constants';
import { useEntityData } from '../../../EntityContext';
import analytics, { EventType, EntityActionType } from '../../../../../analytics';
import { EntityHealthStatus } from './EntityHealthStatus';
import { getLocaleTimezone } from '../../../../../shared/time/timeUtils';
import EntityDropdown, { EntityMenuItems } from '../../../EntityDropdown/EntityDropdown';
import PlatformContent from './PlatformContent';
import { getPlatformName } from '../../../utils';
import { useGetAuthenticatedUser } from '../../../../../useGetAuthenticatedUser';
import { EntityType, PlatformPrivileges } from '../../../../../../types.generated';
import EntityCount from './EntityCount';
import EntityName from './EntityName';
import CopyUrn from '../../../../../shared/CopyUrn';

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

const DeprecatedContainer = styled.div`
    width: 110px;
    height: 18px;
    border: 1px solid #ef5b5b;
    border-radius: 15px;
    display: flex;
    justify-content: center;
    align-items: center;
    color: #ef5b5b;
    margin-left: 15px;
    padding-top: 12px;
    padding-bottom: 12px;
`;

const DeprecatedText = styled.div`
    color: #ef5b5b;
    margin-left: 5px;
`;

const DeprecatedTitle = styled(Typography.Text)`
    display: block;
    font-size: 14px;
    margin-bottom: 5px;
    font-weight: bold;
`;

const DeprecatedSubTitle = styled(Typography.Text)`
    display: block;
    margin-bottom: 5px;
`;

const LastEvaluatedAtLabel = styled.div`
    padding: 0;
    margin: 0;
    display: flex;
    align-items: center;
    color: ${ANTD_GRAY[7]};
`;

const Divider = styled.div`
    border-top: 1px solid #f0f0f0;
    padding-top: 5px;
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

function getCanEditName(entityType: EntityType, privileges?: PlatformPrivileges) {
    switch (entityType) {
        case EntityType.GlossaryTerm:
        case EntityType.GlossaryNode:
            return privileges?.manageGlossaries;
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
    const me = useGetAuthenticatedUser();
    const [copiedUrn, setCopiedUrn] = useState(false);
    const basePlatformName = getPlatformName(entityData);
    const platformName = capitalizeFirstLetterOnly(basePlatformName);
    const externalUrl = entityData?.externalUrl || undefined;
    const entityCount = entityData?.entityCount;
    const hasExternalUrl = !!externalUrl;

    const sendAnalytics = () => {
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.ClickExternalUrl,
            entityType,
            entityUrn: urn,
        });
    };

    /**
     * Deprecation Decommission Timestamp
     */
    const localeTimezone = getLocaleTimezone();
    const decommissionTimeLocal =
        (entityData?.deprecation?.decommissionTime &&
            `Scheduled to be decommissioned on ${moment
                .unix(entityData?.deprecation?.decommissionTime)
                .format('DD/MMM/YYYY')} (${localeTimezone})`) ||
        undefined;
    const decommissionTimeGMT =
        entityData?.deprecation?.decommissionTime &&
        moment.unix(entityData?.deprecation?.decommissionTime).utc().format('dddd, DD/MMM/YYYY HH:mm:ss z');

    const hasDetails = entityData?.deprecation?.note !== '' || entityData?.deprecation?.decommissionTime !== null;
    const isDividerNeeded = entityData?.deprecation?.note !== '' && entityData?.deprecation?.decommissionTime !== null;
    const canEditName = isNameEditable && getCanEditName(entityType, me?.platformPrivileges as PlatformPrivileges);

    return (
        <HeaderContainer>
            <MainHeaderContent>
                <PlatformContent />
                <TitleWrapper>
                    <EntityName isNameEditable={canEditName} />
                    {entityData?.deprecation?.deprecated && (
                        <Popover
                            overlayStyle={{ maxWidth: 240 }}
                            placement="right"
                            content={
                                hasDetails ? (
                                    <>
                                        {entityData?.deprecation?.note !== '' && (
                                            <DeprecatedTitle>Note</DeprecatedTitle>
                                        )}
                                        {isDividerNeeded && <Divider />}
                                        {entityData?.deprecation?.note !== '' && (
                                            <DeprecatedSubTitle>{entityData?.deprecation?.note}</DeprecatedSubTitle>
                                        )}
                                        {entityData?.deprecation?.decommissionTime !== null && (
                                            <Typography.Text type="secondary">
                                                <Tooltip placement="right" title={decommissionTimeGMT}>
                                                    <LastEvaluatedAtLabel>{decommissionTimeLocal}</LastEvaluatedAtLabel>
                                                </Tooltip>
                                            </Typography.Text>
                                        )}
                                    </>
                                ) : (
                                    'No additional details'
                                )
                            }
                        >
                            <DeprecatedContainer>
                                <InfoCircleOutlined />
                                <DeprecatedText>Deprecated</DeprecatedText>
                            </DeprecatedContainer>
                        </Popover>
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
                    <CopyUrn urn={urn} isActive={copiedUrn} onClick={() => setCopiedUrn(true)} />
                    {headerDropdownItems && (
                        <EntityDropdown
                            menuItems={headerDropdownItems}
                            refreshBrowser={refreshBrowser}
                            platformPrivileges={me?.platformPrivileges as PlatformPrivileges}
                        />
                    )}
                </TopButtonsWrapper>
                {hasExternalUrl && (
                    <Button href={externalUrl} onClick={sendAnalytics}>
                        View in {platformName}
                        <RightOutlined style={{ fontSize: 12 }} />
                    </Button>
                )}
            </SideHeaderContent>
        </HeaderContainer>
    );
};
