import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import ChangeTypePill from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/components/ChangeTypePill';
import useGetUserName from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/useGetUserName';
import { Popover, Text } from '@src/alchemy-components';
import { CorpUser, Operation } from '@src/types.generated';
import dayjs from '@utils/dayjs';

// dayjs format token (localized date + time), not user-visible text.
const TIMESTAMP_FORMAT = 'll LTS';

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
    const { t } = useTranslation('entity.profile.stats');
    const timestamp = dayjs(operation.lastUpdatedTimestamp);

    const getUserName = useGetUserName();

    return (
        <Content>
            <ContentRow>
                <ChangeTypePill operation={operation} />
                <Text>
                    <Trans
                        t={t}
                        i18nKey="changeHistoryTimeline.byUser"
                        values={{ name: getUserName(user) }}
                        components={{ gray: <Text color="gray" type="span" /> }}
                    />
                </Text>
            </ContentRow>
            <Popover content={timestamp.format(TIMESTAMP_FORMAT)} placement="right">
                <TimeRow>
                    <Text color="gray" type="span" size="sm">
                        {timestamp.fromNow()}
                    </Text>
                </TimeRow>
            </Popover>
        </Content>
    );
}
