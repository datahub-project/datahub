import { LoadingOutlined } from '@ant-design/icons';
import { useIsThemeV2 } from '@src/app/useIsThemeV2';
import React, { useContext, useEffect } from 'react';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
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
    const isShowNavBarRedesign = useShowNavBarRedesign();

    useEffect(() => {
        setFormMode(mode);
    }, [mode, setFormMode]);

    return (
        <ContentContainer $showV1Styles={!isThemeV2} isShowNavBarRedesign={isShowNavBarRedesign}>
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
