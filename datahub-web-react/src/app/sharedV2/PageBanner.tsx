import { Button, colors } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

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

interface Props {
    content: React.ReactNode;
    localStorageKey: string;
    icon?: React.ReactNode;
    backgroundColor?: string;
    exitColor?: FontColorOptions;
}

export default function PageBanner({
    content,
    localStorageKey,
    icon,
    backgroundColor = colors.red[0],
    exitColor = 'red',
}: Props) {
    const [isBannerHidden, setIsBannerHidden] = useState(!!localStorage.getItem(localStorageKey));

    function handleClose() {
        localStorage.setItem(localStorageKey, 'true');
        setIsBannerHidden(true);
    }

    if (isBannerHidden) return null;

    return (
        <BannerWrapper $backgroundColor={backgroundColor}>
            <IconTextWrapper>
                {icon}
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
