import { EntityType } from '../../../types.generated';
import { BrowsePathResolver } from '../browsePathHelper';
import { dataFlowBrowsePaths, filterDataFlowByPath } from '../searchResult/dataFlowSearchResult';

const browsePathResolver = new BrowsePathResolver({
    entityType: EntityType.DataFlow,
    paths: dataFlowBrowsePaths,
    filterEntityHandler: filterDataFlowByPath,
});

export default {
    ...browsePathResolver.getBrowse(),
};
