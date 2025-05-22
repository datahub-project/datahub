import { Pill, Text } from '@components';
import { Skeleton } from 'antd';
import dayjs from 'dayjs';
import LocalizedFormat from 'dayjs/plugin/localizedFormat';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { abbreviateNumber } from '@src/app/dataviz/utils';
import { pluralize } from '@src/app/shared/textUtil';

dayjs.extend(LocalizedFormat);

const HeaderContainer = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
`;

const PillSkeleton = styled(Skeleton.Button)`
    .ant-skeleton-button {
        height: 28px;
        width: 50px;
    }
`;

type TimelineHeaderProps = {
    day?: string | null;
    numberOfChanges: number;
    loading?: boolean;
};

export default function TimelineHeader({ day, numberOfChanges, loading }: TimelineHeaderProps) {
    const formattedDay = useMemo(() => dayjs(day).format('LL'), [day]);

    if (!day) return null;

    return (
        <HeaderContainer>
            <Text weight="bold" size="lg">
                {formattedDay}
            </Text>
            {loading ? (
                <PillSkeleton shape="round" active />
            ) : (
                <Pill
                    label={`${abbreviateNumber(numberOfChanges)} ${pluralize(numberOfChanges, 'Change')}`}
                    color="violet"
                    size="xs"
                    clickable={false}
                />
            )}
        </HeaderContainer>
    );
}
