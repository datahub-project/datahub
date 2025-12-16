import { Text } from '@components';
import { Image } from 'antd';
import React from 'react';
import styled, { useTheme } from 'styled-components';

const HeaderContainer = styled.div`
    display: flex;
    gap: 13px;
    align-items: center;
    padding: 28px 20px 0 20px;
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

export default function ModalHeader() {
    const themeConfig = useTheme();

    return (
        <HeaderContainer>
            <LogoImage src={themeConfig.assets?.logoUrl} preview={false} />
            <HeaderText>
                <Text size="3xl" color="gray" colorLevel={600} weight="bold" lineHeight="normal">
                    Welcome to Datahub
                </Text>
                <Text size="lg" color="gray" colorLevel={1700} lineHeight="normal">
                    Before we get started we just have a few questions
                </Text>
            </HeaderText>
        </HeaderContainer>
    );
}
