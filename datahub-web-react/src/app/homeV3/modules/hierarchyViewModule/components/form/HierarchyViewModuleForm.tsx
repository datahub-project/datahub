/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Input } from '@components';
import React from 'react';
import styled from 'styled-components';

import RelatedEntitiesSection from '@app/homeV3/modules/hierarchyViewModule/components/form/sections/relatedEntities/RelatedEntitiesSection';
import SelectAssetsSection from '@app/homeV3/modules/hierarchyViewModule/components/form/sections/selectAssets/SelectAssetsSection';
import FormItem from '@app/homeV3/modules/shared/Form/FormItem';

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
                <Input
                    label="Name"
                    placeholder="Choose a name for your module"
                    isRequired
                    data-testid="hierarchy-module-name"
                />
            </FormItem>

            <SelectAssetsSection />
            <RelatedEntitiesSection />
        </FormWrapper>
    );
}
