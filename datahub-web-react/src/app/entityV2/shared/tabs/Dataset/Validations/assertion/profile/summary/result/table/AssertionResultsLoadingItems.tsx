import React from 'react';

import styled from 'styled-components';
import { Skeleton, Timeline } from 'antd';
import { range } from 'remirror';

import { AssertionResultType } from '../../../../../../../../../../../types.generated';
import { getResultDotIcon } from '../../../../../assertionUtils';
import { ANTD_GRAY } from '../../../../../../../../constants';

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
