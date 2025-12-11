/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import dayjs from 'dayjs';
import LocalizedFormat from 'dayjs/plugin/localizedFormat';
import React from 'react';
import styled from 'styled-components';

import ChangeTypePill from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/components/ChangeTypePill';
import useGetUserName from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/useGetUserName';
import { Popover, Text } from '@src/alchemy-components';
import { CorpUser, Operation } from '@src/types.generated';

dayjs.extend(LocalizedFormat);

const ContentRow = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
`;

const Content = styled.div`
    position: relative;
    display: flex;
    flex-direction: column;
    gap: 0px;
    top: -8px;
    margin-left: 11px;
`;

const TimeRow = styled.div`
    width: fit-content;
`;

type TimelineContentProps = {
    operation: Operation;
    user: CorpUser;
};

export default function TimelineContent({ operation, user }: TimelineContentProps) {
    const timestamp = dayjs(operation.lastUpdatedTimestamp);

    const getUserName = useGetUserName();

    return (
        <Content>
            <ContentRow>
                <ChangeTypePill operation={operation} />
                <Text>
                    <Text color="gray" type="span">
                        by
                    </Text>{' '}
                    {getUserName(user)}
                </Text>
            </ContentRow>
            <Popover content={timestamp.format('ll LTS')} placement="right">
                <TimeRow>
                    <Text color="gray" type="span" size="sm">
                        {timestamp.fromNow()}
                    </Text>
                </TimeRow>
            </Popover>
        </Content>
    );
}
