import { Form } from 'antd';
import React from 'react';

import ShowRelatedEntitiesSwitch from '@app/homeV3/modules/hierarchyViewModule/components/form/sections/relatedEntities/components/ShowRelatedEntitiesToggler';
import { HierarchyForm } from '@app/homeV3/modules/hierarchyViewModule/components/form/types';

export default function RelatedEntitiesSection() {
    const form = Form.useFormInstance<HierarchyForm>();

    const isChecked = Form.useWatch('showRelatedEntities', form);

    return (
        <Form.Item name="showRelatedEntities">
            <ShowRelatedEntitiesSwitch
                isChecked={isChecked}
                onChange={() => form.setFieldValue('showRelatedEntities', !isChecked)}
            />
        </Form.Item>
    );
}
