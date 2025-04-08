import { Skeleton, Timeline } from 'antd';
import React from 'react';
import { range } from 'remirror';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { getResultDotIcon } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';

import { AssertionResultType } from '@types';

const ItemSkeleton = styled(Skeleton.Input)`
    && {
        width: 100%;
        border-radius: 4px;
        background-color: ${ANTD_GRAY[3]};
    }
`;

export const AssertionResultsLoadingItems = () => {
    return (
        <>
            {range(0, 3).map((index) => (
                <Timeline.Item
                    key={index}
                    dot={<div style={{ opacity: 0.3 }}>{getResultDotIcon(AssertionResultType.Success)}</div>}
                >
                    <ItemSkeleton active />
                </Timeline.Item>
            ))}
        </>
    );
};
