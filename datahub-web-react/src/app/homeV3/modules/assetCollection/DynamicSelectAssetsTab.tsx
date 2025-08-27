import React from 'react';

import LogicalFiltersBuilder from '@app/sharedV2/queryBuilder/LogicalFiltersBuilder';
import { LogicalOperatorType, LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import { properties } from '@app/sharedV2/queryBuilder/properties';

const EMPTY_FILTER = {
    operator: LogicalOperatorType.AND,
    operands: [],
};

type Props = {
    dynamicFilter: LogicalPredicate | null | undefined;
    setDynamicFilter: (newDynamicFilter: LogicalPredicate | null | undefined) => void;
};

const DynamicSelectAssetsTab = ({ dynamicFilter, setDynamicFilter }: Props) => {
    return (
        <LogicalFiltersBuilder
            filters={dynamicFilter ?? EMPTY_FILTER}
            onChangeFilters={setDynamicFilter}
            properties={properties}
        />
    );
};

export default DynamicSelectAssetsTab;
