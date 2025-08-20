import React, { useCallback, useEffect, useState } from 'react';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import AssetPropertiesContext from '@app/entityV2/summary/properties/context/AssetPropertiesContext';
import useAvailableAssetProperties from '@app/entityV2/summary/properties/hooks/useAvailableAssetProperties';
import useInitialAssetProperties from '@app/entityV2/summary/properties/hooks/useInitialAssetProperties';
import { AssetProperty } from '@app/entityV2/summary/properties/types';

interface Props {
    editable: boolean;
}

export default function AssetPropertiesProvider({ children, editable }: React.PropsWithChildren<Props>) {
    const { entityType } = useEntityContext();

    const [isPropertiesInitialized, setIsPropertiesInitialized] = useState<boolean>(false);

    const [properties, setProperties] = useState<AssetProperty[]>([]);

    const { properties: initialProperties, loading } = useInitialAssetProperties(entityType);

    useEffect(() => {
        if (!isPropertiesInitialized && !loading) {
            setProperties(initialProperties);
            setIsPropertiesInitialized(true);
        }
    }, [initialProperties, loading, isPropertiesInitialized]);

    const { availableProperties, availableStructuredProperties } = useAvailableAssetProperties();

    const persistChanges = useCallback((updatedProperties: AssetProperty[]) => {
        // TODO: save changes
        console.log('persist changes: ', updatedProperties);
    }, []);

    const add = useCallback(
        (newProperty: AssetProperty) => {
            const updatedProperties = [...properties, newProperty];
            setProperties(updatedProperties);
            persistChanges(updatedProperties);
        },
        [properties, persistChanges],
    );

    const remove = useCallback(
        (position: number) => {
            const updatedProperties = [...properties];
            updatedProperties.splice(position, 1);
            setProperties(updatedProperties);
            persistChanges(updatedProperties);
        },
        [properties, persistChanges],
    );

    const replace = useCallback(
        (newProperty: AssetProperty, position: number) => {
            const updatedProperties = [...properties];
            updatedProperties.splice(position, 1, newProperty);
            setProperties(updatedProperties);
            persistChanges(updatedProperties);
        },
        [properties, persistChanges],
    );

    return (
        <AssetPropertiesContext.Provider
            value={{
                properties,

                editable,

                availableProperties,
                availableStructuredProperties,

                add,
                remove,
                replace,
            }}
        >
            {children}
        </AssetPropertiesContext.Provider>
    );
}
