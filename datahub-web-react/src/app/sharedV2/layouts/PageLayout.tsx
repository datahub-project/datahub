import { PageTitle } from '@components';
import React from 'react';
import styled from 'styled-components';

import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

const Card = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    background-color: #ffffff;
    display: flex;
    flex-direction: column;
    overflow: hidden;
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    ${(props) => props.$isShowNavBarRedesign && `box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};`}
`;

const PageWrapper = styled(Card)<{ $hasBottomPanel?: boolean }>`
    display: flex;
    width: 100%;
    height: calc(100vh - ${(props) => (props.$hasBottomPanel ? '156px' : '78px')});
    overflow-y: hidden;
`;

const PageTitleWrapper = styled.div`
    padding: 16px 20px 0 20px;
`;

const ContentWrapper = styled.div`
    height: 100%;
    overflow-y: auto;
    padding: 0 20px 20px 20px;
`;

const Panel = styled(Card)`
    padding: 16px;
`;

const SidePannel = styled(Panel)`
    max-width: 30%;
`;

const BottomPanel = styled(Panel)`
    height: 62px;
`;

const VerticalContainer = styled.div`
    display: flex;
    flex: 1;
    flex-direction: column;
    gap: 16px;
`;

const HorizontalContainer = styled.div`
    flex: 1;
    display: flex;
    flex-direction: row;
    gap: 16px;
`;

interface Props {
    title?: string;
    subTitle?: string;
    leftPanelContent?: React.ReactNode;
    rightPanelContent?: React.ReactNode;
    buttomPanelContent?: React.ReactNode;
}

export function PageLayout({
    children,
    title,
    subTitle,
    leftPanelContent,
    rightPanelContent,
    buttomPanelContent,
}: React.PropsWithChildren<Props>) {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    return (
        <VerticalContainer>
            <HorizontalContainer>
                {leftPanelContent && (
                    <SidePannel $isShowNavBarRedesign={isShowNavBarRedesign}>{leftPanelContent}</SidePannel>
                )}

                <PageWrapper $isShowNavBarRedesign={isShowNavBarRedesign} $hasBottomPanel={!!buttomPanelContent}>
                    {title && (
                        <PageTitleWrapper>
                            <PageTitle title={title} subTitle={subTitle} />
                        </PageTitleWrapper>
                    )}
                    <ContentWrapper>{children}</ContentWrapper>
                </PageWrapper>

                {rightPanelContent && (
                    <SidePannel $isShowNavBarRedesign={isShowNavBarRedesign}>{rightPanelContent}</SidePannel>
                )}
            </HorizontalContainer>
            {buttomPanelContent && (
                <BottomPanel $isShowNavBarRedesign={isShowNavBarRedesign}>{buttomPanelContent}</BottomPanel>
            )}
        </VerticalContainer>
    );
}
