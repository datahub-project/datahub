import { useCallback, useState } from 'react';

import { AddModuleHandlerInput } from '@app/homeV3/template/types';

import { DataHubPageModuleType } from '@types';

function useHandleAddModule() {
    const [showAddAssetCollectionModal, setShowAddAssetCollectionModal] = useState(false);

    const onAddModule = useCallback((input: AddModuleHandlerInput) => {
        if (input.module.type === DataHubPageModuleType.AssetCollection) {
            setShowAddAssetCollectionModal(true);
        }
    }, []);

    return { onAddModule, showAddAssetCollectionModal, setShowAddAssetCollectionModal };
}

export default useHandleAddModule;
