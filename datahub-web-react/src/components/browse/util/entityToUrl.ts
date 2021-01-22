// NOTE: this file is a temporary solution. Soon, entity classes will provide the urls via their asBrowseResult method.

import { PageRoutes } from '../../../conf/Global';
import { BrowseResultEntity } from '../../../types.generated';

export function browseEntityResultToUrl(browseResultEntity: BrowseResultEntity) {
    return `${PageRoutes.DATASETS}/${browseResultEntity.urn}`;
}
