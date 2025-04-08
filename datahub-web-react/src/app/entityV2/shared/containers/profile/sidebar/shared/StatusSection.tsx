import { KeyboardArrowDown, KeyboardArrowRight } from '@mui/icons-material';
import { Collapse, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import EntityProperty from '@app/entityV2/shared/containers/profile/sidebar/shared/EntityProperty';
import SyncedOrShared from '@app/entityV2/shared/containers/profile/sidebar/shared/SyncedOrShared';
import TimeProperty from '@app/entityV2/shared/containers/profile/sidebar/shared/TimeProperty';
import { ActionType } from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';
import { getPlatformName } from '@app/entityV2/shared/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

const SyncedAssetContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

export const StyledCollapse = styled(Collapse)`
    text-wrap: wrap;
    .ant-collapse-header {
        padding: 0px 0px !important;
        align-items: center !important;
    }

    .ant-collapse-content-box {
        padding: 0 0 6px 20px !important;
    }

    .ant-collapse-arrow {
        margin-right: 0 !important;
        height: 20px;
        width: 20px;
    }
`;

const StyledIcon = styled.div`
    svg {
        height: 18px;
        width: 18px;
        color: ${REDESIGN_COLORS.DARK_DIVIDER};
        stroke: ${REDESIGN_COLORS.DARK_DIVIDER};
        stroke-width: 1px;
    }
`;

const EmptyText = styled(Typography.Text)`
    color: ${REDESIGN_COLORS.COLD_GREY_TEXT};
`;

const StatusSection = () => {
    const { entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();

    const dataset = entityData as any;
    const entityType = entityData?.type;
    const properties = dataset?.properties;

    const created = properties?.created?.time;
    const lastModified = properties?.lastModified?.time;
    const lastRefreshed = properties?.lastRefreshed;
    const lastOp = dataset?.operations?.length && dataset?.operations[0]?.lastUpdatedTimestamp;
    const lastUpdated = Math.max(lastModified, lastOp);

    const lastIngested = entityData?.lastIngested;
    const platform = entityData?.siblingPlatforms?.[0] || entityData?.platform;
    const rootSiblingPlatformName = getPlatformName(entityData);
    const baseEntityPlatformName = entityData?.platform
        ? entityRegistry.getDisplayName(EntityType.DataPlatform, entityData?.platform)
        : null;

    const source = dataset?.assetOrigin?.resolvedSourceDetails;

    const isDeprecated = entityData?.deprecation?.deprecated;
    const deprecatedByEntity = entityData?.deprecation?.actorEntity;
    const deprecatedByEntityName =
        deprecatedByEntity?.type && entityRegistry.getDisplayName(deprecatedByEntity.type, deprecatedByEntity);
    const decommissionTime = entityData?.deprecation?.decommissionTime;
    const deprecationReplacement = entityData?.deprecation?.replacement;

    const hasTimeProperties = !!(
        created ||
        lastModified ||
        lastRefreshed ||
        lastUpdated ||
        lastIngested ||
        source ||
        isDeprecated
    );

    if (!hasTimeProperties) return null;

    return (
        <SidebarSection
            title="Status"
            content={
                <SyncedAssetContainer>
                    {!!created && <TimeProperty labelText="Created:" time={created} />}
                    {(entityType === EntityType.Dashboard || entityType === EntityType.Chart) && (
                        <>
                            {!!lastModified && <TimeProperty labelText="Last Modified:" time={lastModified} />}
                            {!!lastRefreshed && <TimeProperty labelText="Data Last Refreshed:" time={lastRefreshed} />}
                        </>
                    )}
                    {!!lastUpdated && entityType === EntityType.Dataset && (
                        <TimeProperty
                            labelText="Last Updated:"
                            time={lastUpdated}
                            titleTip={`Time when the asset was last modified ${
                                baseEntityPlatformName ? `in ${baseEntityPlatformName}` : null
                            }`}
                        />
                    )}
                    {isDeprecated && (
                        <StyledCollapse
                            defaultActiveKey=""
                            ghost
                            expandIcon={({ isActive }) => (
                                <StyledIcon>{isActive ? <KeyboardArrowDown /> : <KeyboardArrowRight />} </StyledIcon>
                            )}
                        >
                            <Collapse.Panel
                                header={
                                    <TimeProperty
                                        labelText={`Deprecated${
                                            !!deprecatedByEntityName && `: by ${deprecatedByEntityName}`
                                        }`}
                                    />
                                }
                                key={1}
                            >
                                {deprecationReplacement && (
                                    <EntityProperty labelText="Replacement:" entity={deprecationReplacement} />
                                )}
                                {decommissionTime ? (
                                    <TimeProperty
                                        labelText="Scheduled Decommission:"
                                        time={entityData.deprecation?.decommissionTime}
                                    />
                                ) : (
                                    <EmptyText>No additional information</EmptyText>
                                )}
                            </Collapse.Panel>
                        </StyledCollapse>
                    )}
                    {!!lastIngested && (
                        <SyncedOrShared
                            labelText="Synced:"
                            time={lastIngested}
                            platformName={rootSiblingPlatformName}
                            platform={platform}
                            type={ActionType.SYNC}
                        />
                    )}
                </SyncedAssetContainer>
            }
        />
    );
};

export default StatusSection;
