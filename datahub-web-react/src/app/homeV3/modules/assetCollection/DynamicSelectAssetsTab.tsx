/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import LogicalFiltersBuilder from '@app/sharedV2/queryBuilder/LogicalFiltersBuilder';
import { LogicalOperatorType, LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import { properties } from '@app/sharedV2/queryBuilder/properties';

const EMPTY_FILTER: LogicalPredicate = {
    type: 'logical',
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
