import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import AppLogoLink from '@app/shared/AppLogoLink';

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
    const { t } = useTranslation('entity.form');
    return (
        <StyledDivider>
            <Header>
                <TitleWrapper>
                    <AppLogoLink />
                    <HeaderText>{t('pageTitle')}</HeaderText>
                </TitleWrapper>
            </Header>
        </StyledDivider>
    );
}
