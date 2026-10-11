import React, { useMemo } from 'react';
import styled, { useTheme } from 'styled-components';

import getAvatarColorScheme, { getAvatarColorStyles } from '@components/components/Avatar/utils';

import { PostContent } from '@types';

const PreviewImage = styled.img`
    color: ${(props) => props.theme.colors.textOnFillBrand};
    height: 40px;
    width: 40px;
    border-radius: 11px;
    object-fit: contain;
    background-color: transparent;
    margin-right: 8px;
`;

const PreviewLetter = styled.div<{ $bgColor: string; $textColor: string }>`
    background-color: ${(props) => props.$bgColor};
    font-size: 18px;
    color: ${(props) => props.$textColor};
    height: 40px;
    width: 40px;
    border-radius: 11px;
    display: flex;
    align-items: center;
    justify-content: center;
    margin-right: 8px;
`;

type Props = {
    link: PostContent;
};

export const PinnedLinkLogo = ({ link }: Props) => {
    const theme = useTheme();
    const firstLetter = link?.title?.[0] || link?.description?.[0] || '';
    const scheme = getAvatarColorScheme(firstLetter);
    const avatarStyles = useMemo(() => getAvatarColorStyles(scheme, theme.colors), [scheme, theme.colors]);

    if (!link || !link.link) return null;

    const hasPhoto = !!link?.media?.location;

    return (
        <>
            {(hasPhoto && <PreviewImage src={link.media?.location || undefined} alt={link.title} />) || (
                <PreviewLetter $bgColor={avatarStyles.backgroundColor} $textColor={avatarStyles.color}>
                    {' '}
                    {firstLetter}{' '}
                </PreviewLetter>
            )}
        </>
    );
};
