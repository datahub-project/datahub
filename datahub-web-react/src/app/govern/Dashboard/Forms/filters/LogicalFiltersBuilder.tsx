import { Property } from '@src/app/tests/builder/steps/definition/builder/property/types/properties';
import { LogicalPredicate, PropertyPredicate } from '@src/app/tests/builder/steps/definition/builder/types';
import React from 'react';
import QueryBuilder from './QueryBuilder';

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
