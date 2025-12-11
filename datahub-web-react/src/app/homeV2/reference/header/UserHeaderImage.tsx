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
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

const PreviewImage = styled.img<{ $isShowNavBarRedesign?: boolean }>`
    color: white;
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
    ${(props) => !props.$isShowNavBarRedesign && 'border: 2px solid #ffffff;'}
`;

const PreviewLetter = styled.div<{ color: string; $isShowNavBarRedesign?: boolean }>`
    background-color: ${(props) => props.color};
    font-size: 52px;
    color: white;
    width: 100%;
    display: flex;
    align-items: center;
    justify-content: center;
    border-top-left-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '16px'};
    border-top-right-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '16px'};
    ${(props) => !props.$isShowNavBarRedesign && 'border: 2px solid #ffffff;'}
`;

type Props = {
    photoUrl?: string;
    displayName?: string;
};

export const UserHeaderImage = ({ photoUrl, displayName }: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const hasPhoto = !!photoUrl;
    const firstLetter = displayName?.[0] || '';
    return (
        <>
            {(hasPhoto && (
                <PreviewImage src={photoUrl} alt={displayName} $isShowNavBarRedesign={isShowNavBarRedesign} />
            )) || (
                <PreviewLetter color={getAvatarColor(displayName)} $isShowNavBarRedesign={isShowNavBarRedesign}>
                    {' '}
                    {firstLetter}{' '}
                </PreviewLetter>
            )}
        </>
    );
};
