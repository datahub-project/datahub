import { UpCircleOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import React, { MouseEventHandler, ReactNode } from 'react';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { SEARCH_COLORS } from '../../entityV2/shared/constants';
import { BaseButton, BodyContainer, BodyGridExpander, RotatingButton } from '../../shared/components';

const Layout = styled.div`
    margin-left: 8px;
`;

const HeaderContainer = styled.div``;

type ExpandableNodeProps = {
    isOpen: boolean;
    header: ReactNode;
    body: ReactNode;
    style?: any;
};

const ExpandableNode = ({ isOpen, header, body, style }: ExpandableNodeProps) => {
    return (
        <Layout style={style}>
            <HeaderContainer>{header}</HeaderContainer>
            <BodyGridExpander isOpen={isOpen}>
                <BodyContainer>{body}</BodyContainer>
            </BodyGridExpander>
        </Layout>
    );
};

ExpandableNode.Header = styled.div<{
    isOpen: boolean;
    isSelected?: boolean;
    showBorder?: boolean;
}>`
    display: flex;
    align-items: center;
    justify-content: start;
    cursor: pointer;
    user-select: none;
    padding: 4px;
    gap: 4px;
    border-bottom: 1px solid ${(props) => (props.isOpen || !props.showBorder ? 'transparent' : ANTD_GRAY[4])};
`;

ExpandableNode.SelectableHeader = styled(ExpandableNode.Header)<{ $isSelected: boolean }>`
    & {
        border: 1px solid ${(props) => (props.$isSelected ? SEARCH_COLORS.BACKGROUND_PURPLE : 'transparent')};
        background-color: ${(props) => (props.$isSelected ? SEARCH_COLORS.BACKGROUND_PURPLE : 'transparent')};
        border-radius: 8px;
        overflow: hidden;
    }

    &:hover {
        background-color: ${SEARCH_COLORS.BACKGROUND_PURPLE};
    }
`;

ExpandableNode.HeaderLeft = styled.div`
    display: flex;
    align-items: center;
    flex-grow: 1;
    overflow: hidden;

    > span > a {
        display: none;
    }
    &:hover > span > a {
        display: block;
    }
`;

const ChevronRightIconStyle = styled(ChevronRightIcon)<{ isVisible?: boolean }>`
    &&& {
        color: ${ANTD_GRAY[6]};
        visibility: ${(props) => (props.isVisible ? 'visible' : 'hidden')};
        font-size: 18px;
    }
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
    style,
}: {
    isOpen: boolean;
    isVisible: boolean;
    onClick?: () => void;
    dataTestId?: string;
    style?: any;
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
            icon={<ChevronRightIconStyle isVisible={isVisible} />}
            onClick={onClickButton}
            data-testid={dataTestId}
            style={style}
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
const BaseTitleContainer = styled.div<{ depth: number; maxWidth: number; padLeft: boolean; dynamicWidth?: boolean }>`
    ${(props) =>
        props.dynamicWidth
            ? `
    flex-shrink: 1;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    `
            : `
    max-width: ${props.maxWidth - props.depth * 8}px;
    `}
    padding-left: ${(props) => (props.padLeft ? 8 : 0)}px;
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
    dynamicWidth,
}: {
    color: string;
    size: number;
    depth?: number;
    children: ReactNode;
    maxWidth?: number;
    padLeft?: boolean;
    dynamicWidth?: boolean;
}) => {
    return (
        <BaseTitleContainer depth={depth} maxWidth={maxWidth} padLeft={padLeft} dynamicWidth={dynamicWidth}>
            <BaseTitle ellipsis={{ tooltip: { mouseEnterDelay: 1 } }} color={color} size={size}>
                {children}
            </BaseTitle>
        </BaseTitleContainer>
    );
};

ExpandableNode.Body = styled.div`
    width: 100%;
`;

export default ExpandableNode;
