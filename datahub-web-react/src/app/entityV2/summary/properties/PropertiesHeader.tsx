import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import PropertiesWithDividerWrapper from '@app/entityV2/summary/properties/PropertiesWithDividerWrapper';
import AssetPropertiesProvider from '@app/entityV2/summary/properties/context/AssetPropertiesProvider';

export default function PropertiesHeader() {
    const { entityData } = useEntityData();

    const editable = !!entityData?.privileges?.canManageAssetSummary;

    return (
        <AssetPropertiesProvider editable={editable}>
            <PropertiesWithDividerWrapper />
        </AssetPropertiesProvider>
    );
}
