import { Skeleton, Timeline } from '@components';
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

const LOADING_ITEMS = [{ key: '0' }, { key: '1' }, { key: '2' }];

export const AssertionResultsLoadingItems = () => {
    return (
        <Timeline
            items={LOADING_ITEMS}
            renderDot={() => <div style={{ opacity: 0.3 }}>{getResultDotIcon(AssertionResultType.Success)}</div>}
            renderContent={() => <ItemSkeleton active />}
        />
    );
};
