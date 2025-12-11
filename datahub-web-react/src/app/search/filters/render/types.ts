/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { AppConfig, FacetFilter, FacetFilterInput, FacetMetadata } from '@types';

/**
 * The scenario in which filter rendering is required.
 */
export enum FilterScenarioType {
    /**
     * The V1 search experience, including embedded list search.
     */
    SEARCH_V1,
    /**
     * The V2 search + browse experience, NOT inside the "More" dropdown.
     */
    SEARCH_V2_PRIMARY,
    /**
     * The V2 search + browse experience, inside the "More" dropdown.
     */
    SEARCH_V2_SECONDARY,
}

/**
 * Props passed to every filter renderer
 */
export interface FilterRenderProps {
    scenario: FilterScenarioType;
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    config?: AppConfig;
    onChangeFilters: (newFilters: FacetFilter[]) => void;
}
