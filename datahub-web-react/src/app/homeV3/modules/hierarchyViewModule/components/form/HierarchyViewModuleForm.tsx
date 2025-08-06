import { Input } from '@components';
import { Form } from 'antd';
import React from 'react';

import RelatedEntitiesSection from '@app/homeV3/modules/hierarchyViewModule/components/form/sections/relatedEntities/RelatedEntitiesSection';
import SelectAssetsSection from '@app/homeV3/modules/hierarchyViewModule/components/form/sections/selectAssets/SelectAssetsSection';

export default function HierarchyViewModuleForm() {
    return (
        <>
            <Form.Item
                name="name"
                rules={[
                    {
                        required: true,
                        message: 'Please enter the name',
                    },
                ]}
            >
                <Input label="Name" placeholder="Choose a name for your widget" isRequired />
            </Form.Item>

            <SelectAssetsSection />
            <RelatedEntitiesSection />
        </>
    );
}
