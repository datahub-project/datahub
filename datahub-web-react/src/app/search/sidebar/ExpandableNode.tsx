import React, { MouseEventHandler, ReactNode } from 'react';
import styled from 'styled-components';
import { VscTriangleRight } from 'react-icons/vsc';
import { Typography } from 'antd';
import { UpCircleOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { BaseButton, BodyContainer, BodyGridExpander, RotatingButton } from '../../shared/components';

const Layout = styled.div`
    margin-left: 8px;
`;

const HeaderContainer = styled.div``;

type ExpandableNodeProps = {
    isOpen: boolean;
    header: ReactNode;
    body: ReactNode;
};

const ExpandableNode = ({ isOpen, header, body }: ExpandableNodeProps) => {
    return (
        <Layout>
            <HeaderContainer>{header}</HeaderContainer>
            <BodyGridExpander isOpen={isOpen}>
                <BodyContainer>{body}</BodyContainer>
            </BodyGridExpander>
        </Layout>
    );
};

ExpandableNode.Header = styled.div<{ isOpen: boolean; isSelected?: boolean; showBorder?: boolean }>`
    display: flex;
    align-items: center;
    justify-content: space-between;
    cursor: pointer;
    user-select: none;
    padding-top: 2px;
    padding-bottom: 2px;
    padding-right: 2px;
    border-bottom: 1px solid ${(props) => (props.isOpen || !props.showBorder ? 'transparent' : ANTD_GRAY[4])};
`;

ExpandableNode.SelectableHeader = styled(ExpandableNode.Header)<{ isSelected: boolean }>`
    & {
        border: 1px solid ${(props) => (props.isSelected ? props.theme.styles['primary-color'] : 'transparent')};
        background-color: ${(props) => (props.isSelected ? props.theme.styles['primary-color-light'] : 'transparent')};
        border-radius: 8px;
    }

    &:hover {
        background-color: ${(props) => props.theme.styles['primary-color-light']};
    }
`;

ExpandableNode.HeaderLeft = styled.div`
    display: flex;
    align-items: center;
`;

ExpandableNode.StaticButton = ({ icon, onClick }: { icon: JSX.Element; onClick?: () => void }) => {
    const onClickButton: MouseEventHandler = (e) => {
        e.stopPropagation();
        onClick?.();
    };
    return <BaseButton ghost size="small" type="ghost" icon={icon} onClick={onClickButton} />;
};

ExpandableNode.TriangleButton = ({
    isOpen,
    isVisible,
    onClick,
    dataTestId,
}: {
    isOpen: boolean;
    isVisible: boolean;
    onClick?: () => void;
    dataTestId?: string;
}) => {
    const onClickButton: MouseEventHandler = (e) => {
        e.stopPropagation();
        onClick?.();
    };
    return (
        <RotatingButton
            ghost
            size="small"
            type="ghost"
            deg={isOpen ? 90 : 0}
            icon={<VscTriangleRight style={{ color: '#000', visibility: isVisible ? 'visible' : 'hidden' }} />}
            onClick={onClickButton}
            data-testid={dataTestId}
        />
    );
};

ExpandableNode.CircleButton = ({ isOpen, color }: { isOpen: boolean; color: string }) => {
    return (
        <RotatingButton
            ghost
            size="small"
            type="ghost"
            deg={isOpen ? 0 : 180}
            icon={<UpCircleOutlined style={{ color }} />}
        />
    );
};

// Reduce the ellipsis tolerance the deeper we get into the browse path
const BaseTitleContainer = styled.div<{ depth: number; maxWidth: number; padLeft: boolean }>`
    max-width: ${(props) => props.maxWidth - props.depth * 8}px;
    padding-left: ${(props) => (props.padLeft ? 4 : 0)}px;
`;

const BaseTitle = styled(Typography.Text)<{ color: string; size: number }>`
    font-size: ${(props) => props.size}px;
    color: ${(props) => props.color};
`;

ExpandableNode.Title = ({
    color,
    size,
    depth = 0,
    children,
    maxWidth = 200,
    padLeft = false,
}: {
    color: string;
    size: number;
    depth?: number;
    children: ReactNode;
    maxWidth?: number;
    padLeft?: boolean;
}) => {
    return (
        <BaseTitleContainer depth={depth} maxWidth={maxWidth} padLeft={padLeft}>
            <BaseTitle ellipsis={{ tooltip: { mouseEnterDelay: 1 } }} color={color} size={size}>
                {children}
            </BaseTitle>
        </BaseTitleContainer>
    );
};

ExpandableNode.Body = styled.div``;

export default ExpandableNode;
