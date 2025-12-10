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

const PageWrapper = styled(Card)`
    display: flex;
    width: 100%;
    height: 100%;
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
    width: 33.333%;
    flex-shrink: 0;
    height: 100%;
    padding: 0;
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

const HorizontalContainer = styled.div<{ $hasBottomPanel?: boolean }>`
    flex: 1;
    display: flex;
    flex-direction: row;
    gap: 16px;
    max-height: calc(100vh - ${(props) => (props.$hasBottomPanel ? '156px' : '78px')});
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
    bottomPanelContent?: React.ReactNode;
    topBreadcrumb?: React.ReactNode;
}

export function PageLayout({
    children,
    title,
    titlePill,
    subTitle,
    leftPanelContent,
    rightPanelContent,
    bottomPanelContent,
    topBreadcrumb,
}: React.PropsWithChildren<Props>) {
    return (
        <VerticalContainer>
            <HorizontalContainer $hasBottomPanel={!!bottomPanelContent}>
                {leftPanelContent && <SidePannel>{leftPanelContent}</SidePannel>}

                <PageWrapper>
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
            {bottomPanelContent && <BottomPanel>{bottomPanelContent}</BottomPanel>}
        </VerticalContainer>
    );
}
