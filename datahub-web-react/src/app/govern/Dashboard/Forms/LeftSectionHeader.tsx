import React from 'react';
import { useHistory } from 'react-router';
import { BackButton } from '../../../sharedV2/buttons/BackButton';
import { FlexBox, HeaderContainer, HeaderText, SubText } from './styledComponents';

const LeftSectionHeader = () => {
    const history = useHistory();

    const handleGoBack = () => {
        (history as any)?.goBack();
    };
    return (
        <HeaderContainer>
            <BackButton onGoBack={handleGoBack} />
            <FlexBox>
                <HeaderText>Create Form </HeaderText>
                <SubText> Documentation Form</SubText>
            </FlexBox>
        </HeaderContainer>
    );
};

export default LeftSectionHeader;
