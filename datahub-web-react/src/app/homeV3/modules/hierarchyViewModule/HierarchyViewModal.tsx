import { Form } from 'antd';
import React, { useMemo } from 'react';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import BaseModuleModal from '@app/homeV3/moduleModals/modals/BaseModuleModal';
import { ASSET_TYPE_DOMAINS, ASSET_TYPE_GLOSSARY } from '@app/homeV3/modules/hierarchyViewModule/constants';
import { HierarchyFormContextProvider } from '@app/homeV3/modules/hierarchyViewModule/form/HierarchyFormContext';
import HierarchyViewModuleForm from '@app/homeV3/modules/hierarchyViewModule/form/HierarchyViewModuleForm';
import { HierarchyForm } from '@app/homeV3/modules/hierarchyViewModule/form/types';

import { DataHubPageModuleType } from '@types';

export default function HierarchyViewModal() {
    const {
        upsertModule,
        moduleModalState: { position, close, isEditing, initialState },
    } = usePageTemplateContext();

    const [form] = Form.useForm<HierarchyForm>();

    const initialFormState: HierarchyForm = useMemo(() => {
        return {
            name: initialState?.properties.name || '',
            assetsType: ASSET_TYPE_DOMAINS,
            domainAssets: [],
            glossaryAssets: [],
            showRelatedEntities: !!initialState?.properties.params.hierarchyViewParams?.showRelatedEntities,
        };
    }, [initialState]);

    const urn = initialState?.urn;

    const handleUpsertAssetCollectionModule = () => {
        form.validateFields().then((values) => {
            const assetType = values.assetsType;

            let assetUrns: string[] = [];
            if (assetType === ASSET_TYPE_DOMAINS) {
                assetUrns = values.domainAssets ?? [];
            } else if (assetType === ASSET_TYPE_GLOSSARY) {
                assetUrns = values.glossaryAssets ?? [];
            }

            upsertModule({
                urn,
                name: values.name,
                position: position ?? {},
                type: DataHubPageModuleType.Hierarchy,
                params: {
                    hierarchyViewParams: {
                        assetUrns,
                        showRelatedEntities: values.showRelatedEntities,
                    },
                },
            });
            close();
        });
    };

    return (
        <BaseModuleModal
            title={`${isEditing ? 'Edit' : 'Add'} Hierarchy View`}
            subtitle="Create a widget by selecting assets and information that will be shown to your users"
            onUpsert={handleUpsertAssetCollectionModule}
        >
            <Form form={form} initialValues={initialFormState}>
                <HierarchyFormContextProvider initialValues={initialFormState}>
                    <HierarchyViewModuleForm />
                </HierarchyFormContextProvider>
            </Form>
        </BaseModuleModal>
    );
}
