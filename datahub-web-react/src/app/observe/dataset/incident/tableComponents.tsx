import { Text, Tooltip, colors } from '@components';
import { get } from 'lodash';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { AvatarStack } from '@components/components/AvatarStack/AvatarStack';

import analytics, { EventType } from '@app/analytics';
import { buildIncidentUrlSearch, getAssigneeNamesWithAvatarUrl } from '@app/entityV2/shared/tabs/Incident/utils';
import { IncidentWithRelationships } from '@app/observe/dataset/incident/IncidentsByIncidentSummary.utils';
import { getTimeFromNow } from '@app/shared/time/timeUtils';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';

const IncidentTitleWrapper = styled(Link)`
    display: flex;
    flex-direction: column;
    gap: 4px;
    align-items: flex-start;
    color: ${colors.gray[900]};
`;

const TruncatedText = styled(Text)`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    display: block;
    max-width: 380px;
`;

const AssetWrapper = styled(Link)`
    display: flex;
    flex-direction: row;
    gap: 8px;
    align-items: center;
    max-width: 200px;
`;

const getIncidentUrl = (incidentUrn: string, entityUrl: string) => {
    return buildIncidentUrlSearch({ urn: incidentUrn, baseUrl: `${entityUrl}/Incidents` });
};

type IncidentTimestampTooltipProps = {
    record: IncidentWithRelationships;
};

type IncidentTitleTooltipProps = {
    record: IncidentWithRelationships;
};

type IncidentAssetsColumnProps = {
    record: IncidentWithRelationships;
};

type IncidentAssigneesColumnProps = {
    record: IncidentWithRelationships;
};

export const IncidentTimestampTooltip = ({ record }: IncidentTimestampTooltipProps) => {
    const { created, startedAt, incidentStatus } = record;
    const { lastUpdated } = incidentStatus || {};
    const createdTime = created?.time;
    const lastUpdatedTime = lastUpdated?.time;

    // Use lastUpdatedTime if available, otherwise fallback to createdTime
    const displayTime = lastUpdatedTime || createdTime;

    // Build comprehensive tooltip content
    const tooltipParts: string[] = [];
    if (createdTime) {
        tooltipParts.push(`Created: ${new Date(createdTime).toLocaleString()}`);
    }
    if (startedAt && startedAt !== createdTime) {
        tooltipParts.push(`Started: ${new Date(startedAt).toLocaleString()}`);
    }
    if (lastUpdatedTime && lastUpdatedTime !== createdTime) {
        tooltipParts.push(`Last updated: ${new Date(lastUpdatedTime).toLocaleString()}`);
    }

    const tooltipContent =
        tooltipParts.length > 0 ? (
            <div>
                {tooltipParts.map((part) => (
                    <Text key={part} size="sm" color="gray">
                        {part}
                    </Text>
                ))}
            </div>
        ) : (
            'Timestamp information unavailable'
        );

    return (
        <Tooltip title={tooltipContent}>
            <Text weight="medium" size="md" style={{ textAlign: 'right' }}>
                {displayTime ? getTimeFromNow(displayTime) : 'Unknown'}
            </Text>
        </Tooltip>
    );
};

export const IncidentTitleTooltip = ({ record }: IncidentTitleTooltipProps) => {
    const entityRegistry = useEntityRegistry();
    const { title, description, entity } = record;
    const displayTitle = title || description || 'Untitled Incident';

    // Create tooltip content showing full title and description
    const tooltipContent = (
        <div>
            <Text size="sm" weight="medium" color="black">
                {displayTitle}
            </Text>
            {description && description !== displayTitle && (
                <Text size="sm" color="gray" style={{ marginTop: '4px' }}>
                    {description}
                </Text>
            )}
        </div>
    );

    return (
        <Tooltip title={tooltipContent}>
            <IncidentTitleWrapper
                to={getIncidentUrl(record.urn, entityRegistry.getEntityUrl(entity.type, entity.urn))}
                onClick={() => {
                    analytics.event({
                        type: EventType.DatasetHealthClickEvent,
                        tabType: 'IncidentsByIncident',
                        target: 'incident',
                        targetUrn: record.urn,
                    });
                }}
            >
                <TruncatedText weight="medium" size="md">
                    {displayTitle}
                </TruncatedText>
                {description && description !== displayTitle && (
                    <TruncatedText size="sm" color="gray">
                        {description}
                    </TruncatedText>
                )}
            </IncidentTitleWrapper>
        </Tooltip>
    );
};

export const IncidentAssetsColumn = ({ record }: IncidentAssetsColumnProps) => {
    const entityRegistry = useEntityRegistry();

    // Get the first linked asset to display
    const linkedAssets = record.linkedAssets.relationships.map((relationship) => relationship.entity);
    if (linkedAssets.length === 0) {
        return (
            <Text size="sm" color="gray">
                No assets linked
            </Text>
        );
    }

    const firstAsset = linkedAssets[0];
    const firstAssetUrn = firstAsset.urn;
    const firstAssetName = entityRegistry.getDisplayName(firstAsset.type, firstAsset);
    const firstAssetPlatform = get(firstAsset, 'platform', undefined);

    return (
        <AssetWrapper
            to={`${entityRegistry.getEntityUrl(firstAsset.type, firstAssetUrn)}/Incidents`}
            onClick={() => {
                analytics.event({
                    type: EventType.DatasetHealthClickEvent,
                    tabType: 'IncidentsByIncident',
                    target: 'asset_incidents',
                    targetUrn: firstAssetUrn,
                });
            }}
        >
            <PlatformIcon platform={firstAssetPlatform} size={16} />
            <Text
                size="md"
                color="gray"
                colorLevel={500}
                style={{
                    maxWidth: '150px',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                }}
            >
                {firstAssetName || firstAssetUrn || 'Unknown Asset'}
            </Text>
            {linkedAssets.length > 1 && (
                <Text size="sm" color="gray">
                    +{linkedAssets.length - 1} more
                </Text>
            )}
        </AssetWrapper>
    );
};

export const IncidentAssigneesColumn = ({ record }: IncidentAssigneesColumnProps) => {
    const assignees = record.assignees || [];
    if (assignees.length === 0) {
        return null;
    }

    const avatarData = getAssigneeNamesWithAvatarUrl(assignees);

    return (
        <Tooltip
            title={
                <div>
                    {assignees.map((assignee) => (
                        <Text key={assignee.urn} size="sm">
                            {assignee.properties?.displayName || 'Unknown'}
                        </Text>
                    ))}
                </div>
            }
        >
            <div>
                <AvatarStack avatars={avatarData} size="md" maxToShow={3} showRemainingNumber />
            </div>
        </Tooltip>
    );
};
