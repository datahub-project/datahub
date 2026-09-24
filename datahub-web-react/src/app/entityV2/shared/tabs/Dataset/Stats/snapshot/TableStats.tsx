import { Tooltip } from '@components';
import { Typography } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ExpandedActorGroup } from '@app/entityV2/shared/components/styled/ExpandedActorGroup';
import { InfoItem } from '@app/entityV2/shared/components/styled/InfoItem';
import { formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';
import { countFormatter } from '@utils/formatter/index';

import { CorpUser, Maybe, PartitionSpec, PartitionType, UserUsageCounts } from '@types';

type Props = {
    rowCount?: number;
    columnCount?: number;
    queryCount?: number;
    users?: Array<Maybe<UserUsageCounts>>;
    lastUpdatedTime?: string;
    lastReportedTime?: string;
    partitionSpec?: Maybe<PartitionSpec>;
};

const StatSection = styled.div`
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
    padding: 16px 20px;
`;

const StatContainer = styled.div<{ justifyContent }>`
    display: flex;
    position: relative;
    z-index: 1;
    justify-content: ${(props) => props.justifyContent};
    padding: 12px 2px;
`;

export default function TableStats({
    rowCount,
    columnCount,
    queryCount,
    users,
    lastUpdatedTime,
    lastReportedTime,
    partitionSpec,
}: Props) {
    const { t } = useTranslation('entity.profile.stats');
    const { t: tl } = useTranslation('common.labels');
    // If there are less than 4 items, simply stack the stat views.
    const justifyContent = !queryCount && !users ? 'default' : 'space-between';
    const lastReportedTimeString = lastReportedTime || t('tableStats.unknown');
    if (
        !rowCount &&
        !columnCount &&
        !queryCount &&
        !(users && users.length > 0) &&
        !lastUpdatedTime &&
        !lastReportedTime
    ) {
        return null;
    }
    const sortedUsers = users?.slice().sort((a, b) => (b?.count || 0) - (a?.count || 0));

    // we assume if no partition spec is provided, it's a full table
    const isPartitioned = partitionSpec && partitionSpec.type !== PartitionType.FullTable;

    return (
        <StatSection>
            <Typography.Title level={5}>
                {t(isPartitioned ? 'tableStats.partitionStatsForPartition' : 'tableStats.tableStatsTitle', {
                    partition: partitionSpec?.partition,
                })}
            </Typography.Title>
            <StatContainer justifyContent={justifyContent}>
                {rowCount && (
                    <InfoItem title={tl('rows')}>
                        <Tooltip title={formatNumberWithoutAbbreviation(rowCount)} placement="right">
                            <Typography.Text strong style={{ fontSize: 24 }} data-testid="table-stats-rowcount">
                                {countFormatter(rowCount)}
                            </Typography.Text>
                        </Tooltip>
                    </InfoItem>
                )}
                {columnCount && (
                    <InfoItem title={tl('columns')}>
                        <Typography.Text strong style={{ fontSize: 24 }}>
                            {columnCount}
                        </Typography.Text>
                    </InfoItem>
                )}
                {queryCount && (
                    <InfoItem title={t('tableStats.monthlyQueriesLabel')}>
                        <Typography.Text strong style={{ fontSize: 24 }}>
                            {queryCount}
                        </Typography.Text>
                    </InfoItem>
                )}
                {sortedUsers && sortedUsers.length > 0 && (
                    <InfoItem title={t('tableStats.topUsersLabel')} width="inherit">
                        <div style={{ paddingTop: 8 }}>
                            <ExpandedActorGroup
                                containerStyle={{
                                    justifyContent: 'left',
                                }}
                                actors={
                                    sortedUsers
                                        .filter((user) => user && user?.user !== undefined && user?.user !== null)
                                        .map((user) => user?.user as CorpUser) || []
                                }
                                max={4}
                            />
                        </div>
                    </InfoItem>
                )}
                {lastUpdatedTime && (
                    <InfoItem title={t('tableStats.lastUpdatedLabel')} width="220px">
                        <Tooltip title={t('tableStats.lastReportedAt', { time: lastReportedTimeString })}>
                            <Typography.Text strong style={{ fontSize: 16 }}>
                                {lastUpdatedTime}
                            </Typography.Text>
                        </Tooltip>
                    </InfoItem>
                )}
            </StatContainer>
        </StatSection>
    );
}
