import { Text } from '@components';
import { Form } from 'antd';
import React, { useCallback } from 'react';

import DomainsSelectableTreeView from '@app/homeV3/modules/hierarchyViewModule/components/domains/DomainsSelectableTreeView';
import { useHierarchyFormContext } from '@app/homeV3/modules/hierarchyViewModule/components/form/HierarchyFormContext';
import { FORM_FIELD_ASSET_TYPE } from '@app/homeV3/modules/hierarchyViewModule/components/form/constants';
import EntityTypeTabs from '@app/homeV3/modules/hierarchyViewModule/components/form/sections/selectAssets/assetTypeTabs/AssetTypeTabs';
import GlossarySelectableTreeView from '@app/homeV3/modules/hierarchyViewModule/components/glossary/GlossarySelectableTreeView';
import { ASSET_TYPE_DOMAINS, ASSET_TYPE_GLOSSARY } from '@app/homeV3/modules/hierarchyViewModule/constants';

export default function SelectAssetsSection() {
    const form = Form.useFormInstance();

    const {
        initialValues: { assetsType: defaultAssetsType },
    } = useHierarchyFormContext();

    const assetType = Form.useWatch(FORM_FIELD_ASSET_TYPE, form);

    const onTabClick = useCallback(
        (key: string) => {
            form.setFieldValue(FORM_FIELD_ASSET_TYPE, key);
        },
        [form],
    );

    const tabs = [
        {
            key: ASSET_TYPE_DOMAINS,
            label: 'Domains',
            content: (
                <Form.Item name="domainAssets">
                    <DomainsSelectableTreeView />
                </Form.Item>
            ),
        },
        {
            key: ASSET_TYPE_GLOSSARY,
            label: 'Glossary',
            content: (
                <Form.Item name="glossaryAssets">
                    <GlossarySelectableTreeView />
                </Form.Item>
            ),
        },
    ];

    return (
        <>
            <Text weight="bold">Search and Select Assets</Text>
            <Form.Item name={FORM_FIELD_ASSET_TYPE}>
                <EntityTypeTabs tabs={tabs} onTabClick={onTabClick} defaultKey={assetType ?? defaultAssetsType} />
            </Form.Item>
        </>
    );
}
