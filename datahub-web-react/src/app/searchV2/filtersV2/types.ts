/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { FacetFilterInput, FacetMetadata } from '@src/types.generated';

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
