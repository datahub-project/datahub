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
