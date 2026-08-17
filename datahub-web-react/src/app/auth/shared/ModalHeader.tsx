import { Heading, Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

const HeaderContainer = styled.div`
    display: flex;
    gap: 13px;
    align-items: center;
    padding: 8px 20px 4px 20px;
`;

const LogoImage = styled.img`
    width: 58px;
    height: auto;
`;

const HeaderText = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

interface Props {
    subHeading?: string;
}

export default function ModalHeader({ subHeading }: Props) {
    const themeConfig = useTheme();
    const { t } = useTranslation('auth');

    return (
        <HeaderContainer>
            <LogoImage src={themeConfig.assets?.logoUrl} alt="" />
            <HeaderText>
                <Heading type="h1" size="2xl" weight="bold" color="gray" colorLevel={600}>
                    {t('welcomeToDataHub')}
                </Heading>
                {subHeading && (
                    <Text size="lg" color="gray" colorLevel={1700} lineHeight="normal">
                        {subHeading}
                    </Text>
                )}
            </HeaderText>
        </HeaderContainer>
    );
}
