import React from 'react';
import styled from 'styled-components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { RecentActions } from '../content/recent/RecentActions';
import { CenterTabs } from '../content/tabs/CenterTabs';

const Container = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    flex: 1;
    overflow: hidden;
    overflow-y: auto;
    padding: ${(props) => (props.$isShowNavBarRedesign ? '16px 20px' : '10px')};
    ${(props) =>
        !props.$isShowNavBarRedesign &&
        `
        margin-top: 8px;
        height: 100%;
    `}

    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        margin: 0 5px 0 5px;
        border-radius: ${props.theme.styles['border-radius-navbar-redesign']};
        box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};
        height: calc(100vh - 87px);
        background-color: white;
    `}

    display: flex;
    flex-direction: column;
    /* Hide scrollbar for Chrome, Safari, and Opera */
    &::-webkit-scrollbar {
        display: none;
    }
`;

const Content = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
`;

const Wrapper = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    gap: 16px;
    overflow: hidden;
    padding: 5px 0 5px 0;
`;

export const CenterContent = () => {
    const isShowNavBarRedesign = useShowNavBarRedesign();

    const FinalWrapper = isShowNavBarRedesign ? Wrapper : React.Fragment;

    return (
        <FinalWrapper>
            {isShowNavBarRedesign && <RecentActions />}
            <Container $isShowNavBarRedesign={isShowNavBarRedesign}>
                <Content>
                    {!isShowNavBarRedesign && <RecentActions />}
                    <CenterTabs />
                </Content>
            </Container>
        </FinalWrapper>
    );
};
