import React from 'react';

import BaseCreateModuleModal from '@app/homeV3/addModule/modal/BaseCreateModuleModal';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';

export default function SampleCreateModuleModal() {
    const {
        createModuleModalState: { moduleType, isOpen },
    } = usePageTemplateContext();

    const onCreate = () => console.log('CREATE sample', moduleType);

    return (
        <BaseCreateModuleModal isOpen={isOpen} title="Sample" onCreate={onCreate}>
            moduleType: {moduleType}
        </BaseCreateModuleModal>
    );
}
