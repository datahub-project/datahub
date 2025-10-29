
import { PropertyRow } from '@app/entity/shared/tabs/Properties/types';
import { GenericEntityProperties } from '@app/entity/shared/types';

import { CustomPropertiesEntry } from '@types';

export function mapCustomPropertiesToPropertyRows(customProperties: CustomPropertiesEntry[]) {
    return (customProperties?.map((customProp) => ({
        displayName: customProp.key,
        values: [{ value: customProp.value || '' }],
        type: { type: 'string', nativeDataType: 'string' },
    })) || []) as PropertyRow[];
}

function matchesName(name: string, filterText: string) {
    return name.toLocaleLowerCase().includes(filterText.toLocaleLowerCase());
}

export function getFilteredCustomProperties(filterText: string, entityData?: GenericEntityProperties | null) {
    return entityData?.customProperties?.filter(
        (property) => matchesName(property.key, filterText) || matchesName(property.value || '', filterText),
    );
}
