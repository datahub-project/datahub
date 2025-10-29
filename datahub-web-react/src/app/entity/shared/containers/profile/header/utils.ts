

import { StructuredPropertiesEntry } from '@types';

export function filterForAssetBadge(prop: StructuredPropertiesEntry) {
    return prop.structuredProperty.settings?.showAsAssetBadge && !prop.structuredProperty.settings?.isHidden;
}
