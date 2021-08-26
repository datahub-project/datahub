import { EntityType } from '../../../types.generated';
import { BrowsePathResolver } from '../browsePathHelper';
import { dashboardBrowsePaths, filterDashboardByPath } from '../searchResult/dashboardSearchResult';

const browsePathResolver = new BrowsePathResolver({
    entityType: EntityType.Dashboard,
    paths: dashboardBrowsePaths,
    filterEntityHandler: filterDashboardByPath,
});

export default {
    ...browsePathResolver.getBrowse(),
};
