import { LoadingOutlined } from '@ant-design/icons';
import { useIsThemeV2 } from '@src/app/useIsThemeV2';
import React, { useContext, useEffect } from 'react';
import AddToForm from './AddToForm';
import CreateFormHeader from './CreateFormHeader';
import DetailsForm from './DetailsForm';
import { FormMode } from './formUtils';
import ManageFormContext from './ManageFormContext';
import { ContentContainer, ContentWrapper, StyledSpin } from './styledComponents';

interface Props {
    mode: FormMode;
}

const FormContent = ({ mode }: Props) => {
    const { setFormMode, isFormLoading } = useContext(ManageFormContext);
    const isThemeV2 = useIsThemeV2();

    useEffect(() => {
        setFormMode(mode);
    }, [mode, setFormMode]);

    return (
        <ContentContainer $showV1Styles={!isThemeV2}>
            <ContentWrapper>
                <StyledSpin spinning={isFormLoading} indicator={<LoadingOutlined />}>
                    <CreateFormHeader />
                    <DetailsForm />
                    <AddToForm />
                </StyledSpin>
            </ContentWrapper>
        </ContentContainer>
    );
};

export default FormContent;
