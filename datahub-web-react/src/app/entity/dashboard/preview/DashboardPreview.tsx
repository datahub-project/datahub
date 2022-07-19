import React from 'react';
import styled from 'styled-components';
import { ClockCircleOutlined, EyeOutlined, TeamOutlined } from '@ant-design/icons';
import { Typography } from 'antd';

import {
    AccessLevel,
    Domain,
    Container,
    EntityType,
    GlobalTags,
    GlossaryTerms,
    Owner,
    SearchInsight,
    ParentContainersResult,
    Deprecation,
    DashboardStatsSummary,
} from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { capitalizeFirstLetter } from '../../../shared/textUtil';
import { IconStyleType } from '../../Entity';
import { ANTD_GRAY } from '../../shared/constants';
import { formatNumberWithoutAbbreviation } from '../../../shared/formatNumber';
import { toRelativeTimeString } from '../../../shared/time/timeUtils';
import { PercentileLabel } from '../../shared/stats/PercentileLabel';

const StatText = styled.span`
    color: ${ANTD_GRAY[8]};
`;

export const DashboardPreview = ({
    urn,
    name,
    platformInstanceId,
    description,
    platform,
    access,
    owners,
    tags,
    glossaryTerms,
    domain,
    container,
    insights,
    logoUrl,
    chartCount,
    statsSummary,
    lastUpdatedMs,
    externalUrl,
    parentContainers,
    deprecation,
}: {
    urn: string;
    platform: string;
    platformInstanceId?: string;
    name?: string;
    description?: string | null;
    access?: AccessLevel | null;
    owners?: Array<Owner> | null;
    tags?: GlobalTags;
    glossaryTerms?: GlossaryTerms | null;
    domain?: Domain | null;
    container?: Container | null;
    deprecation?: Deprecation | null;
    insights?: Array<SearchInsight> | null;
    logoUrl?: string | null;
    chartCount?: number | null;
    statsSummary?: DashboardStatsSummary | null;
    lastUpdatedMs?: number | null;
    externalUrl?: string | null;
    parentContainers?: ParentContainersResult | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const capitalizedPlatform = capitalizeFirstLetter(platform);

    // acryl-main only.
    const effectiveViewCount =
        (!!statsSummary?.viewCountLast30Days && statsSummary.viewCountLast30Days) || statsSummary?.viewCount;
    const effectiveViewCountText = (!!statsSummary?.viewCountLast30Days && 'views last month') || 'views';

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Dashboard, urn)}
            name={name || ''}
            description={description || ''}
            type="Dashboard"
            typeIcon={entityRegistry.getIcon(EntityType.Dashboard, 14, IconStyleType.ACCENT)}
            logoUrl={logoUrl || ''}
            platformInstanceId={platformInstanceId}
            platform={capitalizedPlatform}
            qualifier={access}
            owners={owners}
            tags={tags}
            container={container || undefined}
            glossaryTerms={glossaryTerms || undefined}
            domain={domain}
            deprecation={deprecation}
            insights={insights}
            parentContainers={parentContainers}
            externalUrl={externalUrl}
            topUsers={statsSummary?.topUsersLast30Days}
            stats={[
                (chartCount && (
                    <StatText>
                        <b>{chartCount}</b> charts
                    </StatText>
                )) ||
                    undefined,
                (!!effectiveViewCount && (
                    <StatText>
                        <EyeOutlined style={{ marginRight: 8, color: ANTD_GRAY[7] }} />
                        <b>{formatNumberWithoutAbbreviation(effectiveViewCount)}</b> {effectiveViewCountText}{' '}
                        {!!statsSummary?.viewCountPercentileLast30Days && (
                            <Typography.Text type="secondary">
                                -{' '}
                                <PercentileLabel
                                    percentile={statsSummary?.viewCountPercentileLast30Days}
                                    description={`This dashboard has been viewed more often than ${statsSummary?.viewCountPercentileLast30Days}% of similar dashboards in the past 30 days.`}
                                />
                            </Typography.Text>
                        )}
                    </StatText>
                )) ||
                    undefined,
                (!!statsSummary?.uniqueUserCountLast30Days && (
                    <StatText>
                        <TeamOutlined style={{ marginRight: 8, color: ANTD_GRAY[7] }} />
                        <b>{formatNumberWithoutAbbreviation(statsSummary?.uniqueUserCountLast30Days)}</b> unique users{' '}
                        {!!statsSummary?.uniqueUserPercentileLast30Days && (
                            <Typography.Text type="secondary">
                                -{' '}
                                <PercentileLabel
                                    percentile={statsSummary?.uniqueUserPercentileLast30Days}
                                    description={`This dashboard has had more unique users than ${statsSummary?.uniqueUserPercentileLast30Days}% of similar dashboards in the past 30 days.`}
                                />
                            </Typography.Text>
                        )}
                    </StatText>
                )) ||
                    undefined,
                (!!lastUpdatedMs && (
                    <StatText>
                        <ClockCircleOutlined style={{ marginRight: 8, color: ANTD_GRAY[7] }} />
                        Changed {toRelativeTimeString(lastUpdatedMs)}
                    </StatText>
                )) ||
                    undefined,
            ].filter((stat) => stat !== undefined)}
        />
    );
};
