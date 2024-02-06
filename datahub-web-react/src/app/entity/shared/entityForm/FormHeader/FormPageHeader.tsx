import React from 'react';
import styled from 'styled-components';
import AppLogoLink from '../../../../shared/AppLogoLink';

const Header = styled.div`
    padding: 12px 24px;
    background-color: black;
    font-size: 24px;
    display: flex;
    align-items: center;
    color: white;
    justify-content: space-between;
`;

const HeaderText = styled.div`
    margin-left: 24px;
`;

const StyledDivider = styled.div`
    display: flex;
    flex-direction: column;
`;

const TitleWrapper = styled.div`
    display: flex;
    align-items: center;
`;

export default function FormPageHeader() {
    return (
        <StyledDivider>
            <Header>
                <TitleWrapper>
                    <AppLogoLink />
                    <HeaderText>Complete Documentation Requests</HeaderText>
                </TitleWrapper>
            </Header>
        </StyledDivider>
    );
}
