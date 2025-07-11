import React, { useMemo } from 'react';

import SampleCreateModuleModal from '@app/homeV3/addModule/modal/SampleCreateModuleModal';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import CreateHierarchyViewModuleModal from '@app/homeV3/modules/hierarchyViewModule/CreateHierarchyViewModuleModal';

import { DataHubPageModuleType } from '@types';

export default function CreateModuleModal() {
    const {
        createModuleModalState: { moduleType },
    } = usePageTemplateContext();

    const CreateModuleModalComponent = useMemo(() => {
        switch (moduleType) {
            // TODO: add support of other module types
            case DataHubPageModuleType.Hierarchy:
                return CreateHierarchyViewModuleModal;
            case DataHubPageModuleType.AssetCollection:
                return SampleCreateModuleModal;
            default:
                return null;
        }
    }, [moduleType]);

    if (moduleType === undefined) return null;
    if (!CreateModuleModalComponent) return null;

    return <CreateModuleModalComponent />;
}
