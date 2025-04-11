import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { Announcements } from '../action/announcement/Announcements';

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
