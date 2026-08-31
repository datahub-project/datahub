import { Input } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
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
    const { t } = useTranslation('modules');
    return (
        <FormWrapper>
            <FormItem
                name="name"
                rules={[
                    {
                        required: true,
                        message: t('details.nameValidation'),
                    },
                ]}
            >
                <Input
                    label={t('details.nameLabel')}
                    placeholder={t('details.namePlaceholder')}
                    isRequired
                    data-testid="hierarchy-module-name"
                />
            </FormItem>

            <SelectAssetsSection />
            <RelatedEntitiesSection />
        </FormWrapper>
    );
}
