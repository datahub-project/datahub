import { useContext } from 'react';

import AssetPropertiesContext from '@app/entityV2/summary/properties/context/AssetPropertiesContext';

export default function useAssetPropertiesContext() {
    return useContext(AssetPropertiesContext);
}
