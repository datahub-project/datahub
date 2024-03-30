import React from 'react';
import styled from 'styled-components';
import getAvatarColor from '../../../shared/avatar/getAvatarColor';

const PreviewImage = styled.img`
    color: white;
    width: 100%;
    min-height: 240px;
    max-height: 260px;
    height: auto;
    object-fit: cover;
    background-color: transparent;
    border-top-left-radius: 16px;
    border-top-right-radius: 16px;
    border: 2px solid #ffffff;
`;

const PreviewLetter = styled.div<{ color: string }>`
    background-color: ${(props) => props.color};
    font-size: 52px;
    color: white;
    width: 100%;
    display: flex;
    align-items: center;
    justify-content: center;
    border-top-left-radius: 16px;
    border-top-right-radius: 16px;
    border: 2px solid #ffffff;
`;

type Props = {
    photoUrl?: string;
    displayName?: string;
};

export const UserHeaderImage = ({ photoUrl, displayName }: Props) => {
    const hasPhoto = !!photoUrl;
    const firstLetter = displayName?.[0] || '';
    return (
        <>
            {(hasPhoto && <PreviewImage src={photoUrl} alt={displayName} />) || (
                <PreviewLetter color={getAvatarColor(displayName)}> {firstLetter} </PreviewLetter>
            )}
        </>
    );
};
