import React, { useContext } from 'react';
import { useHistory } from 'react-router';
import { BackButton } from '../../../sharedV2/buttons/BackButton';
import { FlexBox, HeaderContainer, HeaderText, SubText } from './styledComponents';
import ManageFormContext from './ManageFormContext';

const LeftSectionHeader = () => {
    const history = useHistory();
    const { formMode } = useContext(ManageFormContext);

    const handleGoBack = () => {
        (history as any)?.goBack();
    };
    return (
        <HeaderContainer>
            <BackButton onGoBack={handleGoBack} />
            <FlexBox>
                <HeaderText>{formMode === 'create' ? 'Create Form' : 'Edit form'} </HeaderText>
                <SubText> Documentation Form</SubText>
            </FlexBox>
        </HeaderContainer>
    );
};

export default LeftSectionHeader;
