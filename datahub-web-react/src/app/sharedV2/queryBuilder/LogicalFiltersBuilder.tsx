import React from 'react';

import QueryBuilder from '@app/sharedV2/queryBuilder/QueryBuilder';
import { Property } from '@app/sharedV2/queryBuilder/builder/property/types/properties';
import { LogicalPredicate, PropertyPredicate } from '@app/sharedV2/queryBuilder/builder/types';

interface Props {
    filters: LogicalPredicate | PropertyPredicate;
    onChangeFilters: (newPredicate?: LogicalPredicate) => void;
    properties: Property[];
}

const LogicalFiltersBuilder = ({ filters, onChangeFilters, properties }: Props) => {
    const clearFilters = () => {
        onChangeFilters(undefined);
    };

    return (
        <QueryBuilder
            selectedPredicate={filters}
            onChangePredicate={onChangeFilters}
            properties={properties}
            clearFilters={clearFilters}
            depth={0}
            index={0}
        />
    );
};

export default LogicalFiltersBuilder;
