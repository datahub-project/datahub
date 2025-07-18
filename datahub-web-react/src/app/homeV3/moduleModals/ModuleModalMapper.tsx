import React, { useMemo } from 'react';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import AssetCollectionModal from '@app/homeV3/modules/assetCollection/AssetCollectionModal';

import { DataHubPageModuleType } from '@types';

export default function ModuleModalMapper() {
    const {
        moduleModalState: { moduleType },
    } = usePageTemplateContext();

    const ModuleModalComponent = useMemo(() => {
        switch (moduleType) {
            // TODO: add support of other module types
            case DataHubPageModuleType.AssetCollection:
                return AssetCollectionModal;
            default:
                return null;
        }
    }, [moduleType]);

    if (moduleType === undefined) return null;
    if (!ModuleModalComponent) return null;

    return <ModuleModalComponent />;
}
