import { BrowsePathResolver } from '@graphql-mock/fixtures/browsePathHelper';
import { datasetBrowsePaths, filterDatasetByPath } from '@graphql-mock/fixtures/searchResult/datasetSearchResult';
import { EntityType } from '@types';

const browsePathResolver = new BrowsePathResolver({
    entityType: EntityType.Dataset,
    paths: datasetBrowsePaths,
    filterEntityHandler: filterDatasetByPath,
});

export default {
    ...browsePathResolver.getBrowse(),
};
