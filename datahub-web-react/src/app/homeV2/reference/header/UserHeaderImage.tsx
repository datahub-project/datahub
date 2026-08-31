import React, { useMemo } from 'react';
import styled, { useTheme } from 'styled-components';

import getAvatarColorScheme, { getAvatarColorStyles } from '@components/components/Avatar/utils';

import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

const PreviewImage = styled.img<{ $isShowNavBarRedesign?: boolean }>`
    color: ${(props) => props.theme.colors.textOnFillBrand};
    width: 100%;
    min-height: 240px;
    max-height: 260px;
    height: auto;
    object-fit: cover;
    background-color: transparent;
    border-top-left-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '16px'};
    border-top-right-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '16px'};
    ${(props) => !props.$isShowNavBarRedesign && `border: 2px solid ${props.theme.colors.bg};`}
`;

const PreviewLetter = styled.div<{ $bgColor: string; $isShowNavBarRedesign?: boolean }>`
    background-color: ${(props) => props.$bgColor};
    font-size: 52px;
    color: ${(props) => props.theme.colors.textOnFillBrand};
    width: 100%;
    display: flex;
    align-items: center;
    justify-content: center;
    border-top-left-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '16px'};
    border-top-right-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '16px'};
    ${(props) => !props.$isShowNavBarRedesign && `border: 2px solid ${props.theme.colors.bg};`}
`;

type Props = {
    photoUrl?: string;
    displayName?: string;
};

export const UserHeaderImage = ({ photoUrl, displayName }: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const theme = useTheme();
    const hasPhoto = !!photoUrl;
    const firstLetter = displayName?.[0] || '';
    const scheme = getAvatarColorScheme(displayName || '');
    const avatarStyles = useMemo(() => getAvatarColorStyles(scheme, theme.colors), [scheme, theme.colors]);
    return (
        <>
            {(hasPhoto && (
                <PreviewImage src={photoUrl} alt={displayName} $isShowNavBarRedesign={isShowNavBarRedesign} />
            )) || (
                <PreviewLetter $bgColor={avatarStyles.backgroundColor} $isShowNavBarRedesign={isShowNavBarRedesign}>
                    {' '}
                    {firstLetter}{' '}
                </PreviewLetter>
            )}
        </>
    );
};
