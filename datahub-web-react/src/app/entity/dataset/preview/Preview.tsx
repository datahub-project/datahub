import React from 'react';
import styled from 'styled-components';
import { ClockCircleOutlined, ConsoleSqlOutlined, TableOutlined, TeamOutlined } from '@ant-design/icons';
import {
    EntityType,
    FabricType,
    Owner,
    GlobalTags,
    GlossaryTerms,
    SearchInsight,
    Domain,
    Container,
    ParentContainersResult,
    Maybe,
    Deprecation,
    DatasetStatsSummary,
} from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { capitalizeFirstLetterOnly } from '../../../shared/textUtil';
import { IconStyleType } from '../../Entity';
import { ANTD_GRAY } from '../../shared/constants';
import { toRelativeTimeString } from '../../../shared/time/timeUtils';
import { formatNumberWithoutAbbreviation } from '../../../shared/formatNumber';

const StatText = styled.span`
    color: ${ANTD_GRAY[8]};
`;

export const Preview = ({
    urn,
    name,
    origin,
    description,
    platformName,
    platformLogo,
    platformNames,
    platformLogos,
    platformInstanceId,
    owners,
    globalTags,
    domain,
    deprecation,
    snippet,
    insights,
    glossaryTerms,
    subtype,
    externalUrl,
    container,
    parentContainers,
    rowCount,
    statsSummary,
    lastUpdatedMs,
}: {
    urn: string;
    name: string;
    origin: FabricType;
    description?: string | null;
    platformName: string;
    platformLogo?: string | null;
    platformNames?: (Maybe<string> | undefined)[];
    platformLogos?: (Maybe<string> | undefined)[];
    platformInstanceId?: string;
    owners?: Array<Owner> | null;
    domain?: Domain | null;
    deprecation?: Deprecation | null;
    globalTags?: GlobalTags | null;
    snippet?: React.ReactNode | null;
    insights?: Array<SearchInsight> | null;
    glossaryTerms?: GlossaryTerms | null;
    subtype?: string | null;
    externalUrl?: string | null;
    container?: Container | null;
    parentContainers?: ParentContainersResult | null;
    rowCount?: number | null;
    statsSummary?: DatasetStatsSummary | null;
    lastUpdatedMs?: number | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const capitalPlatformName = capitalizeFirstLetterOnly(platformName);
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Dataset, urn)}
            name={name || ''}
            description={description || ''}
            type={capitalizeFirstLetterOnly(subtype) || 'Dataset'}
            logoUrl={platformLogo || ''}
            typeIcon={entityRegistry.getIcon(EntityType.Dataset, 12, IconStyleType.ACCENT)}
            platform={capitalPlatformName}
            platforms={platformNames}
            logoUrls={platformLogos}
            platformInstanceId={platformInstanceId}
            qualifier={origin}
            tags={globalTags || undefined}
            owners={owners}
            domain={domain}
            container={container || undefined}
            deprecation={deprecation}
            snippet={snippet}
            glossaryTerms={glossaryTerms || undefined}
            insights={insights}
            parentContainers={parentContainers}
            externalUrl={externalUrl}
            topUsers={statsSummary?.topUsersLast30Days}
            stats={[
                (rowCount && (
                    <StatText>
                        <TableOutlined style={{ marginRight: 8, color: ANTD_GRAY[7] }} />
                        <b>{formatNumberWithoutAbbreviation(rowCount)}</b> rows
                    </StatText>
                )) ||
                    undefined,
                (statsSummary?.queryCountLast30Days && (
                    <StatText>
                        <ConsoleSqlOutlined style={{ marginRight: 8, color: ANTD_GRAY[7] }} />
                        <b>{formatNumberWithoutAbbreviation(statsSummary?.queryCountLast30Days)}</b> queries last month
                    </StatText>
                )) ||
                    undefined,
                (statsSummary?.uniqueUserCountLast30Days && (
                    <StatText>
                        <TeamOutlined style={{ marginRight: 8, color: ANTD_GRAY[7] }} />
                        <b>{formatNumberWithoutAbbreviation(statsSummary?.uniqueUserCountLast30Days)}</b> unique users
                    </StatText>
                )) ||
                    undefined,
                (lastUpdatedMs && (
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
