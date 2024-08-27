import { Button, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { GetDatasetQuery } from '../../../../../../../graphql/dataset.generated';
import { DatasetProfile, Operation, UsageQueryResult } from '../../../../../../../types.generated';
import UsageFacepile from '../../../../../dataset/profile/UsageFacepile';
import { ANTD_GRAY } from '../../../../constants';
import { useBaseEntity, useRouteToTab } from '../../../../EntityContext';
import { SidebarHeader } from '../SidebarHeader';
import { InfoItem } from '../../../../components/styled/InfoItem';
import { formatNumberWithoutAbbreviation } from '../../../../../../shared/formatNumber';

const HeaderInfoBody = styled(Typography.Text)`
    font-size: 16px;
    color: ${ANTD_GRAY[9]};
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

    const hasUsageStats = baseEntity?.dataset?.usageStats !== undefined;
    const hasDatasetProfiles = baseEntity?.dataset?.datasetProfiles !== undefined;
    const hasOperations = (baseEntity?.dataset?.operations?.length || 0) > 0;

    const usageStats = (hasUsageStats && (baseEntity?.dataset?.usageStats as UsageQueryResult)) || undefined;
    const datasetProfiles =
        (hasDatasetProfiles && (baseEntity?.dataset?.datasetProfiles as Array<DatasetProfile>)) || undefined;
    const latestProfile = datasetProfiles && datasetProfiles[0];
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
                <SidebarHeader title="Stats" />
                <StatsButton onClick={() => routeToTab({ tabName: 'Stats' })} type="link">
                    More stats &gt;
                </StatsButton>
            </HeaderContainer>
            {/* Dataset Profile Entry */}
            {hasLatestProfiles && (
                <StatsRow>
                    {latestProfile?.rowCount ? (
                        <InfoItem
                            title="Rows"
                            onClick={() => routeToTab({ tabName: 'Queries' })}
                            width={INFO_ITEM_WIDTH_PX}
                        >
                            <HeaderInfoBody>{formatNumberWithoutAbbreviation(latestProfile?.rowCount)}</HeaderInfoBody>
                        </InfoItem>
                    ) : null}
                    {latestProfile?.columnCount ? (
                        <InfoItem title="Columns" width={INFO_ITEM_WIDTH_PX}>
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
                            title="Monthly Queries"
                            onClick={() => routeToTab({ tabName: 'Queries' })}
                            width={INFO_ITEM_WIDTH_PX}
                        >
                            <HeaderInfoBody>{usageStats?.aggregations?.totalSqlQueries}</HeaderInfoBody>
                        </InfoItem>
                    ) : null}
                    {(usageStats?.aggregations?.users?.length || 0) > 0 ? (
                        <InfoItem title="Top Users" width={INFO_ITEM_WIDTH_PX}>
                            <UsageFacepile users={usageStats?.aggregations?.users} maxNumberDisplayed={10} />
                        </InfoItem>
                    ) : null}
                </StatsRow>
            )}
            {/* Operation Entry */}
            {hasLatestOperation ? (
                <StatsRow>
                    <InfoItem
                        title="Last Updated"
                        onClick={() => routeToTab({ tabName: 'Queries' })}
                        width={LAST_UPDATED_WIDTH_PX}
                    >
                        <HeaderInfoBody>{lastUpdatedTime}</HeaderInfoBody>
                    </InfoItem>
                </StatsRow>
            ) : null}
        </div>
    );
};
