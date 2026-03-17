import { Icon } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import { CountStyle } from '@app/entityV2/shared/SidebarStyledComponents';

const Wrapper = styled.div`
    border: none;
    background: transparent;
`;

const Header = styled.div<{ $collapsible?: boolean }>`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 20px;
    cursor: ${(props) => (props.$collapsible ? 'pointer' : 'default')};
    user-select: none;
`;

const HeaderLeft = styled.div`
    display: flex;
    align-items: center;
    min-width: 0;
    gap: 4px;
`;

const HeaderRight = styled.div`
    display: flex;
    align-items: center;
    flex-shrink: 0;
    gap: 4px;
`;

const CaretIcon = styled.div`
    display: flex;
    align-items: center;
    flex-shrink: 0;
    color: ${(props) => props.theme.colors.icon};
`;

const Title = styled.span`
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    color: ${(props) => props.theme.colors.text};
    font-weight: 700;
    font-size: 14px;
    display: flex;
    align-items: center;
`;

const Extra = styled.div`
    cursor: pointer;
`;

const ContentBody = styled.div`
    padding: 4px 20px 0 20px;
`;

type Props = {
    title: string;
    content: React.ReactNode;
    extra?: React.ReactNode;
    count?: number;
    collapsedContent?: React.ReactNode;
    collapsible?: boolean;
    expandedByDefault?: boolean;
    showFullCount?: boolean;
};

export const SidebarSection = ({
    title,
    content,
    extra,
    count = 0,
    collapsedContent,
    collapsible = true,
    expandedByDefault = true,
    showFullCount,
}: Props) => {
    const [isExpanded, setIsExpanded] = useState(expandedByDefault);

    const toggle = () => {
        if (collapsible) setIsExpanded((prev) => !prev);
    };

    return (
        <Wrapper>
            <Header $collapsible={collapsible} onClick={toggle}>
                <HeaderLeft>
                    <Title>{title}</Title>
                    {count > 0 && (
                        <CountStyle>{showFullCount ? <>{count}</> : <>{count > 10 ? '10+' : count}</>}</CountStyle>
                    )}
                    {collapsedContent}
                </HeaderLeft>
                <HeaderRight>
                    {extra && <Extra onClick={(e) => e.stopPropagation()}>{extra}</Extra>}
                    {collapsible && (
                        <CaretIcon>
                            <Icon
                                icon={isExpanded ? 'CaretDown' : 'CaretRight'}
                                source="phosphor"
                                size="md"
                                color="inherit"
                            />
                        </CaretIcon>
                    )}
                </HeaderRight>
            </Header>
            {isExpanded && <ContentBody data-testid={`sidebar-section-content-${title}`}>{content}</ContentBody>}
        </Wrapper>
    );
};
