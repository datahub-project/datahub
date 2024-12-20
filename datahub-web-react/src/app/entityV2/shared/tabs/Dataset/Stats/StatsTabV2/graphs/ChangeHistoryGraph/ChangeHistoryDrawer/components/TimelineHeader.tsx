import { Pill, Text } from '@components';
import { pluralize } from '@src/app/shared/textUtil';
import { Skeleton } from 'antd';
import dayjs from 'dayjs';
import LocalizedFormat from 'dayjs/plugin/localizedFormat';
import React, { useMemo } from 'react';
import styled from 'styled-components';

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
    day: string;
    numberOfChanges: number;
    loading?: boolean;
};

export default function TimelineHeader({ day, numberOfChanges, loading }: TimelineHeaderProps) {
    const formattedDay = useMemo(() => dayjs(day).format('LL'), [day]);

    return (
        <HeaderContainer>
            <Text weight="bold" size="lg">
                {formattedDay}
            </Text>
            {loading ? (
                <PillSkeleton shape="round" active />
            ) : (
                <Pill
                    label={`${numberOfChanges} ${pluralize(numberOfChanges, 'Change')}`}
                    colorScheme="violet"
                    size="xs"
                    clickable={false}
                />
            )}
        </HeaderContainer>
    );
}
