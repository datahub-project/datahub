import React from 'react';

import PropertiesWithDividerWrapper from '@app/entityV2/summary/properties/PropertiesWithDividerWrapper';
import AssetPropertiesProvider from '@app/entityV2/summary/properties/context/AssetPropertiesProvider';

export default function PropertiesHeader() {
    // TODO: use permissions
    const editable = true;

    return (
        <AssetPropertiesProvider editable={editable}>
            <PropertiesWithDividerWrapper />
        </AssetPropertiesProvider>
    );
}
