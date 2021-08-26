import { EntityType } from '../../../types.generated';
import { BrowsePathResolver } from '../browsePathHelper';
import { datasetBrowsePaths, filterDatasetByPath } from '../searchResult/datasetSearchResult';

const browsePathResolver = new BrowsePathResolver({
    entityType: EntityType.Dataset,
    paths: datasetBrowsePaths,
    filterEntityHandler: filterDatasetByPath,
});

export default {
    ...browsePathResolver.getBrowse(),
};
