import { GenericEntityProperties } from '@app/entity/shared/types';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import EntityRegistry from '@src/app/entity/EntityRegistry';

import { EntityType, StructuredPropertiesEntry } from '@types';

export function filterForAssetBadge(prop: StructuredPropertiesEntry) {
    return prop.structuredProperty.settings?.showAsAssetBadge && !prop.structuredProperty.settings?.isHidden;
}
