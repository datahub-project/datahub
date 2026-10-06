import { Text } from '@components';
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

const Heading = styled(Text)`
    color: ${(props) => props.theme.colors.text};
`;

const SubHeading = styled(Text)`
    color: ${(props) => props.theme.colors.textSecondary};
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
                <Heading size="3xl" weight="bold" lineHeight="normal">
                    {t('welcomeToDataHub')}
                </Heading>
                {subHeading && (
                    <SubHeading size="lg" lineHeight="normal">
                        {subHeading}
                    </SubHeading>
                )}
            </HeaderText>
        </HeaderContainer>
    );
}
