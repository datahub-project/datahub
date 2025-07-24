import React, { useMemo } from 'react';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import AssetCollectionModal from '@app/homeV3/modules/assetCollection/AssetCollectionModal';
import DocumentationModal from '@app/homeV3/modules/documentation/DocumentationModal';
import HierarchyViewModal from '@app/homeV3/modules/hierarchyViewModule/HierarchyViewModal';
import LinkModal from '@app/homeV3/modules/link/LinkModal';

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
            case DataHubPageModuleType.Link:
                return LinkModal;
            case DataHubPageModuleType.RichText:
                return DocumentationModal;
            case DataHubPageModuleType.Hierarchy:
                return HierarchyViewModal;
            default:
                return null;
        }
    }, [moduleType]);

    if (moduleType === undefined) return null;
    if (!ModuleModalComponent) return null;

    return <ModuleModalComponent />;
}
