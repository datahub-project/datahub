import { Button, Text, colors } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import { FontColorOptions } from '@components/theme/config';

const BannerWrapper = styled.div<{ $backgroundColor: string }>`
    padding: 12px;
    font-size: 14px;
    font-weight: 500;
    display: flex;
    justify-content: space-between;
    align-items: center;
    background-color: ${({ $backgroundColor }) => $backgroundColor};
    border-radius: 8px;
`;

const IconTextWrapper = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
`;

const StyledButton = styled(Button)`
    padding: 0;
`;

const ActionLink = styled(Text)<{ $color: string }>`
    color: ${({ $color }) => $color};
    cursor: pointer;
    font-weight: 600;
    white-space: nowrap;

    &:hover {
        text-decoration: underline;
    }
`;

interface Props {
    content: React.ReactNode;
    icon?: React.ReactNode;
    backgroundColor?: string;
    /** Action link text. Required when using onAction. */
    actionText?: string;
    /** Action link click handler. If provided, shows action link. Otherwise, shows close button with localStorage dismissal. */
    onAction?: () => void;
    /** Action link color. Only used when onAction is provided. */
    actionColor?: string;
    /** Key for localStorage to persist dismissal. Only used when onAction is not provided. */
    localStorageKey?: string;
    /** Color for the close button icon. Only used when onAction is not provided. */
    exitColor?: FontColorOptions;
}

export default function PageBanner({
    content,
    icon,
    backgroundColor = colors.red[0],
    actionText,
    onAction,
    actionColor = colors.blue[1000],
    localStorageKey,
    exitColor = 'red',
}: Props) {
    const [isBannerHidden, setIsBannerHidden] = useState(
        localStorageKey && !onAction ? !!localStorage.getItem(localStorageKey) : false,
    );

    function handleClose() {
        if (localStorageKey) {
            localStorage.setItem(localStorageKey, 'true');
        }
        setIsBannerHidden(true);
    }

    if (isBannerHidden) return null;

    return (
        <BannerWrapper $backgroundColor={backgroundColor}>
            <IconTextWrapper>
                {icon}
                {content}
            </IconTextWrapper>
            {onAction && actionText ? (
                <ActionLink $color={actionColor} onClick={onAction}>
                    {actionText}
                </ActionLink>
            ) : (
                <StyledButton
                    icon={{ icon: 'X', color: exitColor, source: 'phosphor', size: '2xl' }}
                    variant="link"
                    onClick={handleClose}
                />
            )}
        </BannerWrapper>
    );
}
