import React from 'react';
import styled from 'styled-components';
import { PostContent } from '../../../../../types.generated';
import getAvatarColor from '../../../../shared/avatar/getAvatarColor';

const PreviewImage = styled.img`
    color: white;
    height: 40px;
    width: 40px;
    border-radius: 11px;
    object-fit: contain;
    background-color: transparent;
    margin-right: 4px;
    margin-right: 8px;
`;

const PreviewLetter = styled.div<{ color: string }>`
    background-color: ${(props) => props.color};
    font-size: 18px;
    color: white;
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
    if (!link || !link.link) return null;

    const hasPhoto = !!link?.media?.location;
    const firstLetter = link.title[0] || link.description?.[0] || '';

    return (
        <>
            {(hasPhoto && <PreviewImage src={link.media?.location || undefined} alt={link.title} />) || (
                <PreviewLetter color={getAvatarColor(firstLetter)}> {firstLetter} </PreviewLetter>
            )}
        </>
    );
};
