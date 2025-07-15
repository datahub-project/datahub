import React, { useMemo } from 'react';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import AssetCollectionModal from '@app/homeV3/modules/assetCollection/AssetCollectionModal';

import { DataHubPageModuleType } from '@types';

export default function CreateModuleModalMapper() {
    const {
        createModuleModalState: { moduleType },
    } = usePageTemplateContext();

    const CreateModuleModalComponent = useMemo(() => {
        switch (moduleType) {
            // TODO: add support of other module types
            case DataHubPageModuleType.AssetCollection:
                return AssetCollectionModal;
            default:
                return null;
        }
    }, [moduleType]);

    if (moduleType === undefined) return null;
    if (!CreateModuleModalComponent) return null;

    return <CreateModuleModalComponent />;
}
