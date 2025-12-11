/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import getAvatarColor from '@app/shared/avatar/getAvatarColor';

import { PostContent } from '@types';

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
