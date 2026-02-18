import { Button } from '@components';
import React, { useState } from 'react';
import styled, { useTheme } from 'styled-components';

import { FontColorOptions } from '@components/theme/config';

const BannerWrapper = styled.div<{ $backgroundColor: string }>`
    padding: 16px;
    font-size: 14px;
    font-weight: 500;
    display: flex;
    justify-content: space-between;
    align-items: center;
    background-color: ${({ $backgroundColor }) => $backgroundColor};
`;

const IconTextWrapper = styled.div`
    display: flex;
    gap: 16px;
    align-items: center;
`;

const StyledButton = styled(Button)`
    padding: 0;
`;

const IconWrapper = styled.div`
    flex-shrink: 0;
`;

interface Props {
    content: React.ReactNode;
    localStorageKey: string;
    icon?: React.ReactNode;
    backgroundColor?: string;
    exitColor?: FontColorOptions;
}

export default function PageBanner({ content, localStorageKey, icon, backgroundColor, exitColor = 'red' }: Props) {
    const theme = useTheme();
    const bgColor = backgroundColor || theme.colors.bgSurfaceWarning;
    const [isBannerHidden, setIsBannerHidden] = useState(!!localStorage.getItem(localStorageKey));

    function handleClose() {
        localStorage.setItem(localStorageKey, 'true');
        setIsBannerHidden(true);
    }

    if (isBannerHidden) return null;

    return (
        <BannerWrapper $backgroundColor={bgColor}>
            <IconTextWrapper>
                <IconWrapper>{icon}</IconWrapper>
                {content}
            </IconTextWrapper>
            <StyledButton
                icon={{ icon: 'X', color: exitColor, source: 'phosphor', size: '2xl' }}
                variant="link"
                onClick={handleClose}
            />
        </BannerWrapper>
    );
}
