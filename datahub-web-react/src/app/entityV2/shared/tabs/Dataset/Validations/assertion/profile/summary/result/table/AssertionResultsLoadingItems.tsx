import { Skeleton, Timeline } from '@components';
import React from 'react';

import { getResultDotIcon } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';

import { AssertionResultType } from '@types';

const LOADING_ITEMS = [{ key: '0' }, { key: '1' }, { key: '2' }];

export const AssertionResultsLoadingItems = () => {
    return (
        <Timeline
            items={LOADING_ITEMS}
            renderDot={() => <div style={{ opacity: 0.3 }}>{getResultDotIcon(AssertionResultType.Success)}</div>}
            renderContent={() => <Skeleton.Input active block />}
        />
    );
};
