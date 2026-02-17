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

const SidePanel = styled(Panel)<{ $closed?: boolean }>`
    width: ${({ $closed }) => ($closed ? '0px' : '33.333%')};
    flex-shrink: 0;
    height: 100%;
    padding: 0;
    opacity: ${({ $closed }) => ($closed ? 0 : 1)};
    ${({ $closed }) =>
        $closed
            ? `
            box-shadow: none;
            transition: width 0.4s ease-in-out, opacity 0.2s ease;
        `
            : `
            transition: width 0.4s ease-in-out, opacity 0.8s ease;
        `}
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

const HorizontalContainer = styled.div<{ $hasBottomPanel?: boolean; $isRightPanelCollapsed?: boolean }>`
    flex: 1;
    display: flex;
    flex-direction: row;
    gap: 16px;
    max-height: calc(100vh - ${(props) => (props.$hasBottomPanel ? '156px' : '78px')});
    ${(props) =>
        props.$isRightPanelCollapsed &&
        `
            margin-right: -16px;
        `}
`;

const BreadcrumbContainer = styled.div`
    padding: 16px 20px 0 20px;
`;

interface Props {
    title?: string;
    titlePill?: React.ReactNode;
    subTitle?: string | React.ReactNode;
    leftPanelContent?: React.ReactNode;
    rightPanelContent?: React.ReactNode;
    bottomPanelContent?: React.ReactNode;
    topBreadcrumb?: React.ReactNode;
    isRightPanelCollapsed?: boolean;
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
    isRightPanelCollapsed,
}: React.PropsWithChildren<Props>) {
    return (
        <VerticalContainer>
            <HorizontalContainer $hasBottomPanel={!!bottomPanelContent} $isRightPanelCollapsed={isRightPanelCollapsed}>
                {leftPanelContent && <SidePanel>{leftPanelContent}</SidePanel>}

                <PageWrapper>
                    {topBreadcrumb && <BreadcrumbContainer>{topBreadcrumb}</BreadcrumbContainer>}
                    {title && (
                        <PageTitleWrapper>
                            <PageTitle title={title} subTitle={subTitle} titlePill={titlePill} />
                        </PageTitleWrapper>
                    )}
                    <ContentWrapper>{children}</ContentWrapper>
                </PageWrapper>

                {rightPanelContent && <SidePanel $closed={isRightPanelCollapsed}>{rightPanelContent}</SidePanel>}
            </HorizontalContainer>
            {bottomPanelContent && <BottomPanel>{bottomPanelContent}</BottomPanel>}
        </VerticalContainer>
    );
}
