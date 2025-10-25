import { Form } from 'antd';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import BaseModuleModal from '@app/homeV3/moduleModals/common/BaseModuleModal';
import ModuleDetailsForm from '@app/homeV3/moduleModals/common/ModuleDetailsForm';
import SelectAssetsSection from '@app/homeV3/modules/assetCollection/SelectAssetsSection';
import { SELECT_ASSET_TYPE_DYNAMIC, SELECT_ASSET_TYPE_MANUAL } from '@app/homeV3/modules/assetCollection/constants';
import { LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import { isEmptyLogicalPredicate } from '@app/sharedV2/queryBuilder/builder/utils';

import { DataHubPageModuleType } from '@types';

const ModalContent = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
`;

const AssetCollectionModal = () => {
    const {
        upsertModule,
        moduleModalState: { position, close, isEditing, initialState },
    } = usePageTemplateContext();
    const [form] = Form.useForm();
    const currentName = initialState?.properties?.name || '';
    const currentAssets = (initialState?.properties?.params?.assetCollectionParams?.assetUrns || []).filter(
        (urn): urn is string => typeof urn === 'string',
    );
    const urn = initialState?.urn;

    const currentDynamicFilterLogicalPredicate: LogicalPredicate | undefined = useMemo(
        () =>
            initialState?.properties?.params?.assetCollectionParams?.dynamicFilterJson
                ? JSON.parse(initialState.properties.params.assetCollectionParams.dynamicFilterJson)
                : undefined,
        [initialState?.properties?.params?.assetCollectionParams?.dynamicFilterJson],
    );

    const currentSelectAssetType = useMemo(() => {
        if (currentAssets.length === 0 && currentDynamicFilterLogicalPredicate) {
            return SELECT_ASSET_TYPE_DYNAMIC;
        }
        return SELECT_ASSET_TYPE_MANUAL;
    }, [currentDynamicFilterLogicalPredicate, currentAssets]);

    const [selectAssetType, setSelectAssetType] = useState<string>(currentSelectAssetType);

    const [dynamicFilter, setDynamicFilter] = useState<LogicalPredicate | null | undefined>(
        currentDynamicFilterLogicalPredicate,
    );

    const [selectedAssetUrns, setSelectedAssetUrns] = useState<string[]>(currentAssets);

    const nameValue = Form.useWatch('name', form);

    const isDisabled = useMemo(() => {
        if (!nameValue?.trim()) return true;

        switch (selectAssetType) {
            case SELECT_ASSET_TYPE_MANUAL:
                return !selectedAssetUrns.length;
            case SELECT_ASSET_TYPE_DYNAMIC:
                return isEmptyLogicalPredicate(dynamicFilter);
            default:
                return true;
        }
    }, [nameValue, selectAssetType, selectedAssetUrns, dynamicFilter]);

    const handleUpsertAssetCollectionModule = () => {
        const getAssetCollectionParams = () => {
            switch (selectAssetType) {
                case SELECT_ASSET_TYPE_MANUAL:
                    return {
                        assetUrns: selectedAssetUrns,
                        dynamicFilterJson: undefined,
                    };
                case SELECT_ASSET_TYPE_DYNAMIC:
                    return {
                        assetUrns: [],
                        dynamicFilterJson: JSON.stringify(dynamicFilter),
                    };
                default:
                    return {};
            }
        };

        form.validateFields().then((values) => {
            const { name } = values;

            upsertModule({
                urn,
                name,
                position: position ?? {},
                // scope: initialState?.properties.visibility.scope || undefined,
                type: DataHubPageModuleType.AssetCollection,
                params: {
                    assetCollectionParams: getAssetCollectionParams(),
                },
            });
            close();
        });
    };

    return (
        <BaseModuleModal
            title={`${isEditing ? 'Edit' : 'Add'} Asset Collection`}
            subtitle="Create a module by selecting assets and information that will be shown to your users"
            onUpsert={handleUpsertAssetCollectionModule}
            submitButtonProps={{ disabled: isDisabled }}
            bodyStyles={{ overflow: 'hidden' }}
        >
            <ModalContent>
                <ModuleDetailsForm form={form} formValues={{ name: currentName }} />
                <SelectAssetsSection
                    selectAssetType={selectAssetType}
                    setSelectAssetType={setSelectAssetType}
                    selectedAssetUrns={selectedAssetUrns}
                    setSelectedAssetUrns={setSelectedAssetUrns}
                    dynamicFilter={dynamicFilter}
                    setDynamicFilter={setDynamicFilter}
                />
            </ModalContent>
        </BaseModuleModal>
    );
};

export default AssetCollectionModal;
