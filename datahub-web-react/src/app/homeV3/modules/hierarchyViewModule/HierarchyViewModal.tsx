import { Form } from 'antd';
import React, { useMemo } from 'react';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import BaseModuleModal from '@app/homeV3/moduleModals/common/BaseModuleModal';
import { HierarchyFormContextProvider } from '@app/homeV3/modules/hierarchyViewModule/components/form/HierarchyFormContext';
import HierarchyViewModuleForm from '@app/homeV3/modules/hierarchyViewModule/components/form/HierarchyViewModuleForm';
import { HierarchyForm } from '@app/homeV3/modules/hierarchyViewModule/components/form/types';
import { ASSET_TYPE_DOMAINS, ASSET_TYPE_GLOSSARY } from '@app/homeV3/modules/hierarchyViewModule/constants';
import { filterAssetUrnsByAssetType, getAssetTypeFromAssetUrns } from '@app/homeV3/modules/hierarchyViewModule/utils';

import { DataHubPageModuleType } from '@types';

export default function HierarchyViewModal() {
    const {
        upsertModule,
        moduleModalState: { position, close, isEditing, initialState },
    } = usePageTemplateContext();

    const [form] = Form.useForm<HierarchyForm>();

    const initialFormState: HierarchyForm = useMemo(() => {
        const originalAssetUrns = initialState?.properties.params.hierarchyViewParams?.assetUrns;
        const assetType = getAssetTypeFromAssetUrns(originalAssetUrns);
        const assetUrns = filterAssetUrnsByAssetType(originalAssetUrns, assetType);

        const relatedEntitiesFilterJson =
            initialState?.properties.params.hierarchyViewParams?.relatedEntitiesFilterJson;

            return {
            name: initialState?.properties.name || '',
            assetsType: assetType,
            domainAssets: assetType === ASSET_TYPE_DOMAINS ? assetUrns : [],
            glossaryAssets: assetType === ASSET_TYPE_GLOSSARY ? assetUrns : [],
            showRelatedEntities: !!initialState?.properties.params.hierarchyViewParams?.showRelatedEntities,
            relatedEntitiesFilter: relatedEntitiesFilterJson ? JSON.parse(relatedEntitiesFilterJson) : undefined,
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
                        relatedEntitiesFilterJson: values.relatedEntitiesFilter
                            ? JSON.stringify(values.relatedEntitiesFilter)
                            : undefined,
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
            maxWidth="900px"
        >
            <Form form={form} initialValues={initialFormState}>
                <HierarchyFormContextProvider initialValues={initialFormState}>
                    <HierarchyViewModuleForm />
                </HierarchyFormContextProvider>
            </Form>
        </BaseModuleModal>
    );
}
