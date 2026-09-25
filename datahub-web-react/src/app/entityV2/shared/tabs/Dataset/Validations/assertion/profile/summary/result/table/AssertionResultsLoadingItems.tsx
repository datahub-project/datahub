import { Skeleton, Timeline } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { getResultDotIcon } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';

import { AssertionResultType } from '@types';

const ItemSkeleton = styled(Skeleton.Input)`
    && {
        width: 100%;
        border-radius: 4px;
        background-color: ${(props) => props.theme.colors.bgSurface};
    }
`;

export const AssertionResultsLoadingItems = () => {
    return (
        <>
            {[0, 1, 2].map((index) => (
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
