/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import {
    DomainFilter,
    EntityTypeFilter,
    OwnerFilter,
    PlatformEntityFilter,
    TagFilter,
} from '@app/searchV2/filtersV2/filters';
import FiltersRegistry from '@app/searchV2/filtersV2/filtersRegistry/filtersRegistry';
import {
    DOMAINS_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    OWNERS_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    TAGS_FILTER_NAME,
} from '@app/searchV2/utils/constants';

const defaultFiltersRegistry = new FiltersRegistry();

defaultFiltersRegistry.register(PLATFORM_FILTER_NAME, PlatformEntityFilter);
defaultFiltersRegistry.register(ENTITY_SUB_TYPE_FILTER_NAME, EntityTypeFilter);
defaultFiltersRegistry.register(OWNERS_FILTER_NAME, OwnerFilter);
defaultFiltersRegistry.register(TAGS_FILTER_NAME, TagFilter);
defaultFiltersRegistry.register(DOMAINS_FILTER_NAME, DomainFilter);

export default defaultFiltersRegistry;
