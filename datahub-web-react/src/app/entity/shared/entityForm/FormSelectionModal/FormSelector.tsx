import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { useEntityData } from '../../EntityContext';
import { getFormAssociations } from '../../containers/profile/sidebar/FormInfo/utils';
import FormItem from './FormItem';

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
    const { entityData } = useEntityData();
    const formAssociations = getFormAssociations(entityData);

    return (
        <FormSelectorWrapper>
            <HeaderText>Choose Which Form to View</HeaderText>
            <Subheader>
                There are multiple open requests for this entity. Choose which one you’d like to view or complete.
            </Subheader>
            {formAssociations.map((formAssociation, index) => (
                <div key={formAssociation.form.urn}>
                    <FormItem formAssociation={formAssociation} selectFormUrn={selectFormUrn} />
                    {index !== formAssociations.length - 1 && <StyledDivider />}
                </div>
            ))}
        </FormSelectorWrapper>
    );
}
