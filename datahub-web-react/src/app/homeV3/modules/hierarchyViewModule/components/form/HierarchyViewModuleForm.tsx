import { Input } from '@components';
import React from 'react';
import styled from 'styled-components';

import FormItem from '@app/homeV3/modules/hierarchyViewModule/components/form/components/FormItem';
import RelatedEntitiesSection from '@app/homeV3/modules/hierarchyViewModule/components/form/sections/relatedEntities/RelatedEntitiesSection';
import SelectAssetsSection from '@app/homeV3/modules/hierarchyViewModule/components/form/sections/selectAssets/SelectAssetsSection';

const FormWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

export default function HierarchyViewModuleForm() {
    return (
        <FormWrapper>
            <FormItem
                name="name"
                rules={[
                    {
                        required: true,
                        message: 'Please enter the name',
                    },
                ]}
            >
                <Input label="Name" placeholder="Choose a name for your module" isRequired />
            </FormItem>

            <SelectAssetsSection />
            <RelatedEntitiesSection />
        </FormWrapper>
    );
}
