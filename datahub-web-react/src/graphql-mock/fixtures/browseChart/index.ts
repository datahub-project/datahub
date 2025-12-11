/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { BrowsePathResolver } from '@graphql-mock/fixtures/browsePathHelper';
import { chartBrowsePaths, filterChartByPath } from '@graphql-mock/fixtures/searchResult/chartSearchResult';
import { EntityType } from '@types';

const browsePathResolver = new BrowsePathResolver({
    entityType: EntityType.Chart,
    paths: chartBrowsePaths,
    filterEntityHandler: filterChartByPath,
});

export default {
    ...browsePathResolver.getBrowse(),
};
