import { Divider } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { getFormAssociations } from '@app/entity/shared/containers/profile/sidebar/FormInfo/utils';
import FormItem from '@app/entity/shared/entityForm/FormSelectionModal/FormItem';

const FormSelectorWrapper = styled.div`
    font-size: 14px;
`;

const HeaderText = styled.div`
    font-size: 16px;
    font-weight: 600;
    margin-bottom: 8px;
`;

const Subheader = styled.div`
    margin-bottom: 8px;
`;

const StyledDivider = styled(Divider)`
    margin: 8px 0;
`;

interface Props {
    selectFormUrn: (urn: string) => void;
}

export default function FormSelector({ selectFormUrn }: Props) {
    const { t } = useTranslation('entity.form');
    const { entityData } = useEntityData();
    const formAssociations = getFormAssociations(entityData);

    return (
        <FormSelectorWrapper>
            <HeaderText>{t('selectFormTitle')}</HeaderText>
            <Subheader>{t('selectFormDescription')}</Subheader>
            {formAssociations.map((formAssociation, index) => (
                <div key={formAssociation.form.urn}>
                    <FormItem formAssociation={formAssociation} selectFormUrn={selectFormUrn} />
                    {index !== formAssociations.length - 1 && <StyledDivider />}
                </div>
            ))}
        </FormSelectorWrapper>
    );
}
