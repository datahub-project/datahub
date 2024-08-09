import { LoadingOutlined } from '@ant-design/icons';
import React, { useContext, useEffect } from 'react';
import AddToForm from './AddToForm';
import CreateFormHeader from './CreateFormHeader';
import DetailsForm from './DetailsForm';
import { FormMode } from './formUtils';
import ManageFormContext from './ManageFormContext';
import { ContentContainer, StyledSpin } from './styledComponents';

interface Props {
    mode: FormMode;
}

const FormContent = ({ mode }: Props) => {
    const { setFormMode, isFormLoading } = useContext(ManageFormContext);

    useEffect(() => {
        setFormMode(mode);
    }, [mode, setFormMode]);

    return (
        <ContentContainer>
            <StyledSpin spinning={isFormLoading} indicator={<LoadingOutlined />}>
                <CreateFormHeader />
                <DetailsForm />
                <AddToForm />
            </StyledSpin>
        </ContentContainer>
    );
};

export default FormContent;
