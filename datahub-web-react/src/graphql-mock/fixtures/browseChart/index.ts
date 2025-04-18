import { EntityType } from '../../../types.generated';
import { BrowsePathResolver } from '../browsePathHelper';
import { chartBrowsePaths, filterChartByPath } from '../searchResult/chartSearchResult';

const browsePathResolver = new BrowsePathResolver({
    entityType: EntityType.Chart,
    paths: chartBrowsePaths,
    filterEntityHandler: filterChartByPath,
});

export default {
    ...browsePathResolver.getBrowse(),
};
