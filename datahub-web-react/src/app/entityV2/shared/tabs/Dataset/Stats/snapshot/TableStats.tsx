import { Heading, Tooltip } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ExpandedActorGroup } from '@app/entityV2/shared/components/styled/ExpandedActorGroup';
import { InfoItem } from '@app/entityV2/shared/components/styled/InfoItem';
import { formatNumberWithoutAbbreviation } from '@app/sharedV2/formatNumber';
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

const SectionTitleContainer = styled.div`
    margin-bottom: 8px;
    word-break: break-word;
`;

// A plain span rather than alchemy Text: Tooltip children must forward their ref to a DOM node
const StatValue = styled.span<{ $fontSize: number }>`
    color: ${(props) => props.theme.colors.text};
    font-size: ${(props) => props.$fontSize}px;
    font-weight: 600;
    word-break: break-word;
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
            <SectionTitleContainer>
                <Heading type="h5" size="lg" weight="semiBold">
                    {t(isPartitioned ? 'tableStats.partitionStatsForPartition' : 'tableStats.tableStatsTitle', {
                        partition: partitionSpec?.partition,
                    })}
                </Heading>
            </SectionTitleContainer>
            <StatContainer justifyContent={justifyContent}>
                {rowCount && (
                    <InfoItem title={tl('rows')}>
                        <Tooltip title={formatNumberWithoutAbbreviation(rowCount)} placement="right">
                            <StatValue $fontSize={24} data-testid="table-stats-rowcount">
                                {countFormatter(rowCount)}
                            </StatValue>
                        </Tooltip>
                    </InfoItem>
                )}
                {columnCount && (
                    <InfoItem title={tl('columns')}>
                        <StatValue $fontSize={24}>{columnCount}</StatValue>
                    </InfoItem>
                )}
                {queryCount && (
                    <InfoItem title={t('tableStats.monthlyQueriesLabel')}>
                        <StatValue $fontSize={24}>{queryCount}</StatValue>
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
                            <StatValue $fontSize={16}>{lastUpdatedTime}</StatValue>
                        </Tooltip>
                    </InfoItem>
                )}
            </StatContainer>
        </StatSection>
    );
}
