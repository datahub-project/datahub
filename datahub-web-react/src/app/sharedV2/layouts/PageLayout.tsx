import { PageTitle } from '@components';
import React from 'react';
import styled from 'styled-components';

const Card = styled.div`
    background-color: #ffffff;
    display: flex;
    flex-direction: column;
    overflow: hidden;
    border-radius: ${(props) => props.theme.styles['border-radius-navbar-redesign']};
    box-shadow: ${(props) => props.theme.styles['box-shadow-navbar-redesign']};
`;

const PageWrapper = styled(Card)<{ $hasBottomPanel?: boolean }>`
    display: flex;
    width: 100%;
    height: calc(100vh - ${(props) => (props.$hasBottomPanel ? '156px' : '78px')});
    overflow-y: hidden;
`;

const PageTitleWrapper = styled.div`
    padding: 16px 20px;
`;

const ContentWrapper = styled.div`
    height: 100%;
    overflow-y: auto;
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

const BreadcrumbContainer = styled.div`
    padding: 16px 20px 0 20px;
`;

interface Props {
    title?: string;
    titlePill?: React.ReactNode;
    subTitle?: string;
    leftPanelContent?: React.ReactNode;
    rightPanelContent?: React.ReactNode;
    buttomPanelContent?: React.ReactNode;
    topBreadcrumb?: React.ReactNode;
}

export function PageLayout({
    children,
    title,
    titlePill,
    subTitle,
    leftPanelContent,
    rightPanelContent,
    buttomPanelContent,
    topBreadcrumb,
}: React.PropsWithChildren<Props>) {
    return (
        <VerticalContainer>
            <HorizontalContainer>
                {leftPanelContent && <SidePannel>{leftPanelContent}</SidePannel>}

                <PageWrapper $hasBottomPanel={!!buttomPanelContent}>
                    {topBreadcrumb && <BreadcrumbContainer>{topBreadcrumb}</BreadcrumbContainer>}
                    {title && (
                        <PageTitleWrapper>
                            <PageTitle title={title} subTitle={subTitle} titlePill={titlePill} />
                        </PageTitleWrapper>
                    )}
                    <ContentWrapper>{children}</ContentWrapper>
                </PageWrapper>

                {rightPanelContent && <SidePannel>{rightPanelContent}</SidePannel>}
            </HorizontalContainer>
            {buttomPanelContent && <BottomPanel>{buttomPanelContent}</BottomPanel>}
        </VerticalContainer>
    );
}
