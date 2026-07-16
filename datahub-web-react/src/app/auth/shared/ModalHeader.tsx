import { Text } from '@components';
import { Image } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

const HeaderContainer = styled.div`
    display: flex;
    gap: 13px;
    align-items: center;
    padding: 8px 20px 4px 20px;
`;

const LogoImage = styled(Image)`
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
            <LogoImage src={themeConfig.assets?.logoUrl} preview={false} />
            <HeaderText>
                <Text size="3xl" color="gray" colorLevel={600} weight="bold" lineHeight="normal">
                    {t('welcomeToDataHub')}
                </Text>
                {subHeading && (
                    <Text size="lg" color="gray" colorLevel={1700} lineHeight="normal">
                        {subHeading}
                    </Text>
                )}
            </HeaderText>
        </HeaderContainer>
    );
}
