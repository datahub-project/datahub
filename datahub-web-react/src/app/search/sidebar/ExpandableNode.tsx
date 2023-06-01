import React, { MouseEventHandler, ReactNode } from 'react';
import styled from 'styled-components';
import { VscTriangleRight } from 'react-icons/vsc';
import { Button } from 'antd';
import { UpCircleOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../entity/shared/constants';

const Layout = styled.div`
    margin-left: 8px;
`;

const HeaderContainer = styled.div``;

const BodyGridExpander = styled.div<{ isOpen: boolean }>`
    display: grid;
    grid-template-rows: ${(props) => (props.isOpen ? '1fr' : '0fr')};
    transition: grid-template-rows 250ms;
    overflow: hidden;
`;

const BodyContainer = styled.div`
    min-height: 0;
`;

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
    padding: 2px 4px 2px 4px;
    border-bottom: 1px solid ${(props) => (props.isOpen || !props.showBorder ? 'transparent' : ANTD_GRAY[4])};
`;

ExpandableNode.SelectableHeader = styled(ExpandableNode.Header)<{ isSelected: boolean }>`
    && {
        border: 1px solid ${(props) => (props.isSelected ? props.theme.styles['primary-color'] : 'transparent')};
        background-color: ${(props) => (props.isSelected ? props.theme.styles['primary-color-light'] : 'transparent')};
        border-radius: 8px;
        transition: box-shadow 100ms ease-in-out;
        box-shadow: 'none';
    }
    &&:hover {
        box-shadow: ${(props) => props.theme.styles['box-shadow-hover']};
    }
`;

ExpandableNode.HeaderLeft = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
`;

ExpandableNode.BaseButton = styled(Button)`
    display: flex;
    align-items: center;
    justify-content: center;
    border: none;
    box-shadow: none;
`;

ExpandableNode.RotatingButton = styled(ExpandableNode.BaseButton)<{ deg: number }>`
    transform: rotate(${(props) => props.deg}deg);
    transition: transform 250ms;
`;

ExpandableNode.StaticButton = ({ icon }: { icon: JSX.Element }) => {
    return <ExpandableNode.BaseButton ghost size="small" type="ghost" icon={icon} />;
};

ExpandableNode.TriangleButton = ({
    isOpen,
    isVisible,
    onClick,
}: {
    isOpen: boolean;
    isVisible: boolean;
    onClick?: () => void;
}) => {
    const onClickButton: MouseEventHandler = (e) => {
        e.stopPropagation();
        onClick?.();
    };
    return (
        <ExpandableNode.RotatingButton
            ghost
            size="small"
            type="ghost"
            deg={isOpen ? 90 : 0}
            icon={<VscTriangleRight style={{ color: ANTD_GRAY[9], visibility: isVisible ? 'visible' : 'hidden' }} />}
            onClick={onClickButton}
        />
    );
};

ExpandableNode.CircleButton = ({ isOpen }: { isOpen: boolean }) => {
    return (
        <ExpandableNode.RotatingButton
            ghost
            size="small"
            type="ghost"
            deg={isOpen ? 0 : 180}
            icon={<UpCircleOutlined style={{ color: isOpen ? ANTD_GRAY[9] : ANTD_GRAY[7] }} />}
        />
    );
};

ExpandableNode.Body = styled.div``;

export default ExpandableNode;
