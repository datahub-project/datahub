/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import useGetSearchQueryInputs from '@app/searchV2/useGetSearchQueryInputs';

export type SidebarFilters = Pick<
    ReturnType<typeof useGetSearchQueryInputs>,
    'entityFilters' | 'query' | 'orFilters' | 'viewUrn'
>;

export enum BrowseMode {
    /**
     * Browse by entity type
     */
    ENTITY_TYPE = 'ENTITY_TYPE',
    /**
     * Browse by platform
     */
    PLATFORM = 'PLATFORM',
}
