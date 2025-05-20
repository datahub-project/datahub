import { BrowsePathResolver } from '@graphql-mock/fixtures/browsePathHelper';
import { dataFlowBrowsePaths, filterDataFlowByPath } from '@graphql-mock/fixtures/searchResult/dataFlowSearchResult';
import { EntityType } from '@types';

const browsePathResolver = new BrowsePathResolver({
    entityType: EntityType.DataFlow,
    paths: dataFlowBrowsePaths,
    filterEntityHandler: filterDataFlowByPath,
});

export default {
    ...browsePathResolver.getBrowse(),
};
