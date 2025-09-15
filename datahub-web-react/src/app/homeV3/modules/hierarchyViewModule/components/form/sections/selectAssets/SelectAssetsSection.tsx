import { Text } from '@components';
import { Form } from 'antd';
import React, { useCallback } from 'react';
import styled from 'styled-components';

import DomainsSelectableTreeView from '@app/homeV3/modules/hierarchyViewModule/components/domains/DomainsSelectableTreeView';
import { useHierarchyFormContext } from '@app/homeV3/modules/hierarchyViewModule/components/form/HierarchyFormContext';
import { FORM_FIELD_ASSET_TYPE } from '@app/homeV3/modules/hierarchyViewModule/components/form/constants';
import GlossarySelectableTreeView from '@app/homeV3/modules/hierarchyViewModule/components/glossary/GlossarySelectableTreeView';
import { ASSET_TYPE_DOMAINS, ASSET_TYPE_GLOSSARY } from '@app/homeV3/modules/hierarchyViewModule/constants';
import ButtonTabs from '@app/homeV3/modules/shared/ButtonTabs/ButtonTabs';
import FormItem from '@app/homeV3/modules/shared/Form/FormItem';

const Wrapper = styled.div``;

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
                <FormItem name="domainAssets">
                    <DomainsSelectableTreeView />
                </FormItem>
            ),
        },
        {
            key: ASSET_TYPE_GLOSSARY,
            label: 'Glossary',
            content: (
                <FormItem name="glossaryAssets">
                    <GlossarySelectableTreeView />
                </FormItem>
            ),
        },
    ];

    return (
        <Wrapper>
            <Text color="gray" weight="bold">
                Search and Select Assets
            </Text>
            <FormItem name={FORM_FIELD_ASSET_TYPE}>
                <ButtonTabs tabs={tabs} onTabClick={onTabClick} defaultKey={assetType ?? defaultAssetsType} />
            </FormItem>
        </Wrapper>
    );
}
