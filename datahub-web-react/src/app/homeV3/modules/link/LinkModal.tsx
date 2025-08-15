import { Form } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import BaseModuleModal from '@app/homeV3/moduleModals/common/BaseModuleModal';
import ModuleDetailsForm from '@app/homeV3/moduleModals/common/ModuleDetailsForm';
import LinkForm from '@app/homeV3/modules/link/LinkForm';

import { DataHubPageModuleType } from '@types';

const ModalContent = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
`;

export default function LinkModal() {
    const {
        upsertModule,
        moduleModalState: { position, close, isEditing, initialState },
    } = usePageTemplateContext();
    const [form] = Form.useForm();
    const currentName = initialState?.properties.name || '';
    const linkParams = initialState?.properties?.params?.linkParams;
    const urn = initialState?.urn;

    const nameValue = Form.useWatch('name', form);
    const linkUrlValue = Form.useWatch('linkUrl', form);
    const isDisabled = !nameValue?.trim() || !linkUrlValue?.trim();

    const handleUpsertDocumentationModule = () => {
        form.validateFields().then((values) => {
            const { name, linkUrl, imageUrl, description } = values;
            upsertModule({
                urn,
                name,
                position: position ?? {},
                type: DataHubPageModuleType.Link,
                params: {
                    linkParams: {
                        linkUrl,
                        imageUrl,
                        description,
                    },
                },
            });
            close();
        });
    };

    return (
        <BaseModuleModal
            title={`${isEditing ? 'Edit' : 'Add'} Quick Link`}
            subtitle="Add links to your home page"
            onUpsert={handleUpsertDocumentationModule}
            width="40%"
            submitButtonProps={{ disabled: isDisabled }}
        >
            <ModalContent>
                <ModuleDetailsForm form={form} formValues={{ name: currentName }} />
                <LinkForm form={form} formValues={linkParams ?? undefined} />
            </ModalContent>
        </BaseModuleModal>
    );
}
