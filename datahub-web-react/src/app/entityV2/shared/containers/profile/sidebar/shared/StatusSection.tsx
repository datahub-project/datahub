import React from 'react';
import styled from 'styled-components';
import { KeyboardArrowDown, KeyboardArrowRight } from '@mui/icons-material';
import { Collapse, Typography } from 'antd';
import { REDESIGN_COLORS } from '../../../../constants';
import { useEntityData } from '../../../../../../entity/shared/EntityContext';
import { getPlatformName } from '../../../../utils';
import TimeProperty from './TimeProperty';
import { DataPlatformInstance, EntityType, SyncMechanism } from '../../../../../../../types.generated';
import SyncedOrShared from './SyncedOrShared';
import { Entity } from '../../../../../Entity';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { ACRYL_PLATFORM, ActionType } from './utils';
import { SidebarSection } from '../SidebarSection';

const SyncedAssetContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

export const StyledCollapse = styled(Collapse)`
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
    const sourceInstance =
        source?.mechanism === SyncMechanism.Share ? (source.source as Entity<DataPlatformInstance>) : undefined;
    const sharedTime = source?.lastModified?.time;
    const instanceName = sourceInstance
        ? entityRegistry.getDisplayName(EntityType.DataPlatformInstance, sourceInstance)
        : undefined;

    const isDeprecated = entityData?.deprecation?.deprecated;
    const deprecatedByEntity = entityData?.deprecation?.actorEntity;
    const deprecatedByEntityName =
        deprecatedByEntity?.type && entityRegistry.getDisplayName(deprecatedByEntity.type, deprecatedByEntity);
    const decommissionTime = entityData?.deprecation?.decommissionTime;

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
                    {!sourceInstance && !!lastIngested && (
                        <SyncedOrShared
                            labelText="Synced:"
                            time={lastIngested}
                            platformName={rootSiblingPlatformName}
                            platform={platform}
                            type={ActionType.SYNC}
                        />
                    )}
                    {!!sourceInstance && (
                        <SyncedOrShared
                            labelText="Shared:"
                            time={sharedTime}
                            platformName={ACRYL_PLATFORM}
                            instanceName={instanceName}
                            type={ActionType.SHARE}
                        />
                    )}
                </SyncedAssetContainer>
            }
        />
    );
};

export default StatusSection;
