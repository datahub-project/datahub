import { BrowsePathResolver } from '@graphql-mock/fixtures/browsePathHelper';
import { dashboardBrowsePaths, filterDashboardByPath } from '@graphql-mock/fixtures/searchResult/dashboardSearchResult';
import { EntityType } from '@types';

const browsePathResolver = new BrowsePathResolver({
    entityType: EntityType.Dashboard,
    paths: dashboardBrowsePaths,
    filterEntityHandler: filterDashboardByPath,
});

export default {
    ...browsePathResolver.getBrowse(),
};
