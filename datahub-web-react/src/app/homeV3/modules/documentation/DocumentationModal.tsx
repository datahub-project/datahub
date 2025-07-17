import { Form } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import BaseModuleModal from '@app/homeV3/moduleModals/common/BaseModuleModal';
import ModuleDetailsForm from '@app/homeV3/moduleModals/common/ModuleDetailsForm';
import RichTextContent from '@app/homeV3/modules/documentation/RichTextContent';

import { DataHubPageModuleType } from '@types';

const ModalContent = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
`;

const DocumentationModal = () => {
    const {
        upsertModule,
        moduleModalState: { position, close, isEditing, initialState },
    } = usePageTemplateContext();
    const [form] = Form.useForm();
    const currentName = initialState?.properties.name || '';
    const currentContent = initialState?.properties?.params?.richTextParams?.content;
    const urn = initialState?.urn;

    const handleUpsertDocumentationModule = () => {
        form.validateFields().then((values) => {
            const { name, content } = values;
            upsertModule({
                urn,
                name,
                position: position ?? {},
                type: DataHubPageModuleType.RichText,
                params: {
                    richTextParams: {
                        content,
                    },
                },
            });
            close();
        });
    };

    return (
        <BaseModuleModal
            title={`${isEditing ? 'Edit' : 'Add'} Documentation`}
            subtitle="Document important information for you users"
            onUpsert={handleUpsertDocumentationModule}
        >
            <ModalContent>
                <ModuleDetailsForm form={form} formValues={{ name: currentName }} />
                <RichTextContent content={currentContent} form={form} />
            </ModalContent>
        </BaseModuleModal>
    );
};

export default DocumentationModal;
