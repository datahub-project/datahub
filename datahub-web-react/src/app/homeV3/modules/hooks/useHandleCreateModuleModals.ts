import { useCallback, useState } from 'react';

import { DataHubPageModuleType } from '@types';

function useHandleCreateModuleModals() {
    const [showAddAssetCollectionModal, setShowAddAssetCollectionModal] = useState(false);

    const handleNewModuleModals = useCallback((type: DataHubPageModuleType) => {
        if (type === DataHubPageModuleType.AssetCollection) {
            setShowAddAssetCollectionModal(true);
        }
    }, []);

    return { handleNewModuleModals, showAddAssetCollectionModal, setShowAddAssetCollectionModal };
}

export default useHandleCreateModuleModals;
