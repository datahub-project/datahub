import { LoadingOutlined } from '@ant-design/icons';
import React, { useContext, useEffect } from 'react';

import AddToForm from '@app/govern/Dashboard/Forms/AddToForm';
import CreateFormHeader from '@app/govern/Dashboard/Forms/CreateFormHeader';
import DetailsForm from '@app/govern/Dashboard/Forms/DetailsForm';
import ManageFormContext from '@app/govern/Dashboard/Forms/ManageFormContext';
import { FormMode } from '@app/govern/Dashboard/Forms/formUtils';
import { ContentContainer, ContentWrapper, StyledSpin } from '@app/govern/Dashboard/Forms/styledComponents';
import { useIsThemeV2 } from '@src/app/useIsThemeV2';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

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
        <ContentContainer $showV1Styles={!isThemeV2} $isShowNavBarRedesign={isShowNavBarRedesign}>
            <ContentWrapper $isShowNavBarRedesign={isShowNavBarRedesign}>
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
