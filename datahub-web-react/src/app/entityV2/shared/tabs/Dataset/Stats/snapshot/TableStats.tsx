import { Typography } from 'antd';
import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';
import { CorpUser, Maybe, PartitionSpec, PartitionType, UserUsageCounts } from '../../../../../../../types.generated';
import { countFormatter } from '../../../../../../../utils/formatter/index';
import { formatNumberWithoutAbbreviation } from '../../../../../../shared/formatNumber';
import { ExpandedActorGroup } from '../../../../components/styled/ExpandedActorGroup';
import { InfoItem } from '../../../../components/styled/InfoItem';
import { ANTD_GRAY } from '../../../../constants';

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
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
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
    // If there are less than 4 items, simply stack the stat views.
    const justifyContent = !queryCount && !users ? 'default' : 'space-between';
    const lastReportedTimeString = lastReportedTime || 'unknown';
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
                {isPartitioned ? `Partition Stats for Partition ${partitionSpec.partition}` : 'Table Stats'}
            </Typography.Title>
            <StatContainer justifyContent={justifyContent}>
                {rowCount && (
                    <InfoItem title="Rows">
                        <Tooltip title={formatNumberWithoutAbbreviation(rowCount)} placement="right">
                            <Typography.Text strong style={{ fontSize: 24 }} data-testid="table-stats-rowcount">
                                {countFormatter(rowCount)}
                            </Typography.Text>
                        </Tooltip>
                    </InfoItem>
                )}
                {columnCount && (
                    <InfoItem title="Columns">
                        <Typography.Text strong style={{ fontSize: 24 }}>
                            {columnCount}
                        </Typography.Text>
                    </InfoItem>
                )}
                {queryCount && (
                    <InfoItem title="Monthly Queries">
                        <Typography.Text strong style={{ fontSize: 24 }}>
                            {queryCount}
                        </Typography.Text>
                    </InfoItem>
                )}
                {sortedUsers && sortedUsers.length > 0 && (
                    <InfoItem title="Top Users" width="inherit">
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
                    <InfoItem title="Last Updated" width="220px">
                        <Tooltip title={`Last reported at ${lastReportedTimeString}`}>
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
