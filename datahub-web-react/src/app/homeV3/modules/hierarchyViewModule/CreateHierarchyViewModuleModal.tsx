import { Form } from 'antd';
import React, { useCallback } from 'react';

import BaseCreateModuleModal from '@app/homeV3/addModule/modal/BaseCreateModuleModal';
import { CreateModuleModalProps } from '@app/homeV3/addModule/types';

import HierarchyViewModuleForm from './form/HierarchyViewModuleForm';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { DataHubPageModuleType } from '@types';

interface HierarchyViewModuleFormProps {
    name: string;
    description?: string;
}

export default function CreateHierarchyViewModuleModal() {

    const {createModule, createModuleModalState: {position}} = usePageTemplateContext();
    const [form] = Form.useForm<HierarchyViewModuleFormProps>();

    const onCreate = useCallback(() => {
        console.log('>>> onCreate', form.getFieldsValue());
        form.validateFields();
        createModule({
            name: form.getFieldValue('name'),
            position: position ?? {},
            type: DataHubPageModuleType.Hierarchy,
        })
    }, [form, position, createModule])

    return (
        <BaseCreateModuleModal
            title="Add Hierarchy View"
            subtitle="Create a widget by selecting assets and information that will be shown to your users"
            onCreate={onCreate}
        >
            <Form form={form}>
                <HierarchyViewModuleForm form={form}/>
            </Form>
        </BaseCreateModuleModal>
    );
}
