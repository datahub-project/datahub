import { FacetFilterInput, FacetMetadata } from '@src/types.generated';
import React from 'react';

export type FieldName = string;

export type FacetsGetterResponse = {
    facets?: FacetMetadata[] | undefined;
    loading?: boolean;
};
export type FacetsGetter = (fieldNames: FieldName[]) => FieldToFacetStateMap | undefined;

export type FilterValue = string;

export interface AppliedFieldFilterValue {
    filters: FacetFilterInput[];
}

export type FieldToAppliedFieldFiltersMap = Map<FieldName, AppliedFieldFilterValue>;

export interface FilterComponentProps {
    fieldName: FieldName;
    facetState?: FeildFacetState;
    appliedFilters?: AppliedFieldFilterValue;
    onUpdate?: (value: AppliedFieldFilterValue) => void;
}

export type FilterComponent = React.FC<FilterComponentProps>;

export interface Filter {
    fieldName: FieldName;
    props: FilterComponentProps;
    component: FilterComponent;
}

export interface FiltersRendererProps {
    filters: Filter[];
}

export type FeildFacetState = {
    facet?: FacetMetadata | undefined;
    loading?: boolean;
};

export type FieldToFacetStateMap = Map<FieldName, FeildFacetState>;

export type FieldFacetGetter = (fieldName: FieldName) => FeildFacetState | undefined;

export type FiltersRenderer = React.FC<FiltersRendererProps>;

export type FiltersAppliedHandler = (appliedFilters: FieldToAppliedFieldFiltersMap | undefined) => void;

export type AppliedFieldFilterUpdater = (fieldName: FieldName, value: AppliedFieldFilterValue) => void;
