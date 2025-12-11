/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { Announcements } from '@app/homeV2/action/announcement/Announcements';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

const Container = styled.div<{ $isHidden?: boolean; $isShowNavBarRedesign?: boolean }>`
    overflow-y: auto;

    padding: ${(props) => (props.$isShowNavBarRedesign ? '16px 20px' : '0px 12px 12px 12px')};

    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        margin: 5px;
        border-radius: ${props.theme.styles['border-radius-navbar-redesign']};
        box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};
        background-color: white;
    `}

    /* Hide scrollbar for Chrome, Safari, and Opera */
    &::-webkit-scrollbar {
        display: none;
    }

    ${(props) => props.$isHidden && props.$isShowNavBarRedesign && 'display: none;'}
`;

const Content = styled.div`
    display: flex;
    flex-direction: column;
    gap: 10px;
    min-height: 100%;
`;

export const RightSidebar = () => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const [hasAnnouncements, setHasAnnouncements] = useState<boolean | null>(null);

    const [isSidebarHidden, setIsSidebarHidden] = useState<boolean>(true);

    useEffect(() => {
        if (!isShowNavBarRedesign) return;

        const hasData = hasAnnouncements;
        setIsSidebarHidden(!hasData);
    }, [isShowNavBarRedesign, hasAnnouncements, setIsSidebarHidden]);

    return (
        <Container $isHidden={isSidebarHidden} $isShowNavBarRedesign={isShowNavBarRedesign}>
            <Content>
                <Announcements setHasAnnouncements={setHasAnnouncements} />
            </Content>
        </Container>
    );
};
