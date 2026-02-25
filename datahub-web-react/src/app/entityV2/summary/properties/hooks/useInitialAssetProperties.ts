import { useMemo } from 'react';

import useBasicAssetProperties from '@app/entityV2/summary/properties/hooks/useBasicAssetProperties';
import useAssetProperties from '@app/entityV2/summary/properties/hooks/usePropertiesFromAsset';
import { AssetProperty } from '@app/entityV2/summary/properties/types';

interface Response {
    properties: AssetProperty[];
    loading: boolean;
}

export default function useInitialAssetProperties(entityUrn: string): Response {
    const defaultProperties = useBasicAssetProperties();

    const { assetProperties: entityAssetProperties, loading } = useAssetProperties(entityUrn);

    const properties = useMemo(
        () => entityAssetProperties ?? defaultProperties,
        [entityAssetProperties, defaultProperties],
    );

    return {
        properties,
        loading,
    };
}
