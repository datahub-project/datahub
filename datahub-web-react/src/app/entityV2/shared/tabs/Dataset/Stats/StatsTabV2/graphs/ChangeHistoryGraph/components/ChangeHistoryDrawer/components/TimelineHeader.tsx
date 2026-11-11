import { Pill, Skeleton, Text } from '@components';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { abbreviateNumber } from '@src/app/dataviz/utils';
import dayjs from '@utils/dayjs';

const HeaderContainer = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
`;

const PillSkeleton = styled(Skeleton.Button).attrs({ width: 50, height: 28 })``;

type TimelineHeaderProps = {
    day?: string | null;
    numberOfChanges: number;
    loading?: boolean;
};

export default function TimelineHeader({ day, numberOfChanges, loading }: TimelineHeaderProps) {
    const { t } = useTranslation('entity.profile.stats');
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
                    label={t('timelineHeader.changesCount', {
                        count: numberOfChanges,
                        formattedCount: abbreviateNumber(numberOfChanges),
                    })}
                    color="primary"
                    size="xs"
                    clickable={false}
                />
            )}
        </HeaderContainer>
    );
}
