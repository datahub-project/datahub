import React from 'react';

import LogicalFiltersBuilder from '@app/sharedV2/queryBuilder/LogicalFiltersBuilder';
import { LogicalOperatorType, LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import { properties } from '@app/sharedV2/queryBuilder/properties';

/** Start with one blank condition row so users can immediately pick a property. */
const DEFAULT_FILTER: LogicalPredicate = {
    type: 'logical',
    operator: LogicalOperatorType.AND,
    operands: [{ type: 'property' }],
};

type Props = {
    dynamicFilter: LogicalPredicate | null | undefined;
    setDynamicFilter: (newDynamicFilter: LogicalPredicate | null | undefined) => void;
};

const DynamicSelectAssetsTab = ({ dynamicFilter, setDynamicFilter }: Props) => {
    return (
        <LogicalFiltersBuilder
            filters={dynamicFilter ?? DEFAULT_FILTER}
            onChangeFilters={setDynamicFilter}
            properties={properties}
        />
    );
};

export default DynamicSelectAssetsTab;
