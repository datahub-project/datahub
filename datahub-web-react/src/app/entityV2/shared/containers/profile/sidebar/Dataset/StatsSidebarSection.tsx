import { Button, Typography } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useBaseEntity, useRouteToTab } from '@app/entity/shared/EntityContext';
import UsageFacepile from '@app/entityV2/dataset/profile/UsageFacepile';
import { InfoItem } from '@app/entityV2/shared/components/styled/InfoItem';
import { SidebarHeader } from '@app/entityV2/shared/containers/profile/sidebar/SidebarHeader';
import { formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';

import { GetDatasetQuery } from '@graphql/dataset.generated';
import { Operation, UsageQueryResult } from '@types';

/* eslint-disable i18next/no-literal-string -- route tab name identifiers, not UI text */
const STATS_TAB = 'Stats';
const QUERIES_TAB = 'Queries';
/* eslint-enable i18next/no-literal-string */

const HeaderInfoBody = styled(Typography.Text)`
    font-size: 16px;
    color: ${(props) => props.theme.colors.text};
`;

const HeaderContainer = styled.div`
    justify-content: space-between;
    display: flex;
`;

const StatsButton = styled(Button)`
    margin-top: -4px;
`;

const StatsRow = styled.div`
    padding-top: 12px;
    padding-bottom: 12px;
`;

const INFO_ITEM_WIDTH_PX = '150px';
const LAST_UPDATED_WIDTH_PX = '220px';

export const SidebarStatsSection = () => {
    const { t } = useTranslation('entity.shared.containers');
    const baseEntity = useBaseEntity<GetDatasetQuery>();

    const toLocalDateTimeString = (time: number) => {
        const date = new Date(time);
        return date.toLocaleString([], {
            year: 'numeric',
            month: 'numeric',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            timeZoneName: 'short',
        });
    };

    const latestFullTableProfile = baseEntity?.dataset?.latestFullTableProfile?.[0];
    const latestPartitionProfile = baseEntity?.dataset?.latestPartitionProfile?.[0];

    const hasUsageStats = baseEntity?.dataset?.usageStats !== undefined;
    const hasOperations = (baseEntity?.dataset?.operations?.length || 0) > 0;

    const usageStats = (hasUsageStats && (baseEntity?.dataset?.usageStats as UsageQueryResult)) || undefined;
    const latestProfile = latestFullTableProfile || latestPartitionProfile;

    const operations = (hasOperations && (baseEntity?.dataset?.operations as Array<Operation>)) || undefined;
    const latestOperation = operations && operations[0];

    const lastUpdatedTime = latestOperation && toLocalDateTimeString(latestOperation?.lastUpdatedTimestamp);

    const hasUsageStatsAggregations =
        usageStats?.aggregations?.totalSqlQueries || (usageStats?.aggregations?.users?.length || 0) > 0;
    const hasLatestProfiles = latestProfile?.rowCount || latestProfile?.columnCount;
    const hasLatestOperation = latestOperation?.timestampMillis;
    const routeToTab = useRouteToTab();

    return (
        <div>
            <HeaderContainer>
                <SidebarHeader title={t('sidebar.stats.sectionTitle')} />
                <StatsButton onClick={() => routeToTab({ tabName: STATS_TAB })} type="link">
                    {t('sidebar.stats.moreStatsLink')}
                </StatsButton>
            </HeaderContainer>
            {/* Dataset Profile Entry */}
            {hasLatestProfiles && (
                <StatsRow>
                    {latestProfile?.rowCount ? (
                        <InfoItem
                            title={t('sidebar.stats.rowsLabel')}
                            onClick={() => routeToTab({ tabName: QUERIES_TAB })}
                            width={INFO_ITEM_WIDTH_PX}
                        >
                            <HeaderInfoBody>{formatNumberWithoutAbbreviation(latestProfile?.rowCount)}</HeaderInfoBody>
                        </InfoItem>
                    ) : null}
                    {latestProfile?.columnCount ? (
                        <InfoItem title={t('sidebar.stats.columnsLabel')} width={INFO_ITEM_WIDTH_PX}>
                            <HeaderInfoBody>{latestProfile?.columnCount}</HeaderInfoBody>
                        </InfoItem>
                    ) : null}
                </StatsRow>
            )}
            {/* Usage Stats Entry */}
            {hasUsageStatsAggregations && (
                <StatsRow>
                    {usageStats?.aggregations?.totalSqlQueries ? (
                        <InfoItem
                            title={t('sidebar.stats.monthlyQueriesLabel')}
                            onClick={() => routeToTab({ tabName: QUERIES_TAB })}
                            width={INFO_ITEM_WIDTH_PX}
                        >
                            <HeaderInfoBody>
                                {usageStats?.aggregations?.totalSqlQueries
                                    ? formatNumberWithoutAbbreviation(usageStats?.aggregations?.totalSqlQueries)
                                    : null}
                            </HeaderInfoBody>
                        </InfoItem>
                    ) : null}
                    {(usageStats?.aggregations?.users?.length || 0) > 0 ? (
                        <InfoItem title={t('sidebar.stats.topUsersLabel')} width={INFO_ITEM_WIDTH_PX}>
                            <UsageFacepile users={usageStats?.aggregations?.users} maxNumberDisplayed={10} />
                        </InfoItem>
                    ) : null}
                </StatsRow>
            )}
            {/* Operation Entry */}
            {hasLatestOperation ? (
                <StatsRow>
                    <InfoItem
                        title={t('sidebar.stats.lastUpdatedLabel')}
                        onClick={() => routeToTab({ tabName: QUERIES_TAB })}
                        width={LAST_UPDATED_WIDTH_PX}
                    >
                        <HeaderInfoBody>{lastUpdatedTime}</HeaderInfoBody>
                    </InfoItem>
                </StatsRow>
            ) : null}
        </div>
    );
};
