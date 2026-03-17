import { Icon } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import EntityProperty from '@app/entityV2/shared/containers/profile/sidebar/shared/EntityProperty';
import SyncedOrShared from '@app/entityV2/shared/containers/profile/sidebar/shared/SyncedOrShared';
import TimeProperty from '@app/entityV2/shared/containers/profile/sidebar/shared/TimeProperty';
import { ActionType } from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';
import { getPlatformNameFromEntityData } from '@app/entityV2/shared/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

const SyncedAssetContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

const DeprecatedHeader = styled.div<{ $collapsible: boolean }>`
    display: flex;
    align-items: center;
    gap: 4px;
    cursor: ${(props) => (props.$collapsible ? 'pointer' : 'default')};
`;

const DeprecatedContent = styled.div`
    padding: 0 0 6px 20px;
`;

const EmptyText = styled.span`
    color: ${(props) => props.theme.colors.textTertiary};
`;

const StatusSection = () => {
    const { entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const [isDeprecationExpanded, setIsDeprecationExpanded] = useState(false);

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
    const rootSiblingPlatformName = getPlatformNameFromEntityData(entityData);
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
                        <div>
                            <DeprecatedHeader $collapsible onClick={() => setIsDeprecationExpanded((prev) => !prev)}>
                                <Icon
                                    icon={isDeprecationExpanded ? 'CaretDown' : 'CaretRight'}
                                    source="phosphor"
                                    size="md"
                                    color="inherit"
                                />
                                <TimeProperty
                                    labelText={`Deprecated${
                                        !!deprecatedByEntityName && `: by ${deprecatedByEntityName}`
                                    }`}
                                />
                            </DeprecatedHeader>
                            {isDeprecationExpanded && (
                                <DeprecatedContent>
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
                                </DeprecatedContent>
                            )}
                        </div>
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
