/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
                    color="primary"
                    size="xs"
                    clickable={false}
                />
            )}
        </HeaderContainer>
    );
}
