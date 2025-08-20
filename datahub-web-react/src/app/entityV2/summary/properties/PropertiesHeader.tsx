import React from 'react';

import Properties from '@app/entityV2/summary/properties/components/Properties';
import AssetPropertiesProvider from '@app/entityV2/summary/properties/context/AssetPropertiesProvider';

export default function PropertiesHeader() {
    // TODO: use permissions
    const editable = true;

    return (
        <AssetPropertiesProvider editable={editable}>
            <Properties />
        </AssetPropertiesProvider>
    );
}
