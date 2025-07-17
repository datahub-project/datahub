import { Text } from '@components';
import { Form } from 'antd';
import React, { useCallback } from 'react';

import { ASSET_TYPE_DOMAINS, ASSET_TYPE_GLOSSARY } from '@app/homeV3/modules/hierarchyViewModule/constants';
import { FORM_FIELD_ASSET_TYPE } from '@app/homeV3/modules/hierarchyViewModule/form/constants';
import EntityTypeTabs from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/assetTypeTabs/AssetTypeTabs';
import DomainsTreeView from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/DomainsTreeView';
import GlossaryTreeView from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/GlossaryTreeView';

export default function SelectAssetsSection() {
    const form = Form.useFormInstance();

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
                    <DomainsTreeView />
                </Form.Item>
            ),
        },
        {
            key: ASSET_TYPE_GLOSSARY,
            label: 'Glossary',
            content: (
                <Form.Item name="glossaryAssets">
                    <GlossaryTreeView />
                </Form.Item>
            ),
        },
    ];

    return (
        <>
            <Text weight="bold">Search and Select Assets</Text>
            <Form.Item name={FORM_FIELD_ASSET_TYPE}>
                <EntityTypeTabs tabs={tabs} onTabClick={onTabClick} defaultKey={assetType} />
            </Form.Item>
        </>
    );
}
