import React, { memo, useCallback, useState } from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { VscTriangleDown, VscTriangleRight } from 'react-icons/vsc';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { formatNumber } from '../../shared/formatNumber';
import ExpandableNode from './ExpandableNode';

const Header = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    cursor: pointer;
    user-select: none;
    padding-top: 8px;
`;

const HeaderLeft = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const Title = styled(Typography.Text)`
    font-size: 14px;
    color: ${(props) => props.color};
`;

const Count = styled(Typography.Text)`
    font-size: 12px;
    color: ${(props) => props.color};
`;

const Body = styled.div``;

type Props = {
    environment: string;
    count: number;
};

const EnvironmentNode = ({ environment, count }: Props) => {
    const [isOpen, setIsOpen] = useState<boolean>(false);
    const onClickHeader = useCallback(() => setIsOpen((current) => !current), []);
    const color = ANTD_GRAY[9];

    return (
        <ExpandableNode
            isOpen={isOpen}
            header={
                <Header onClick={onClickHeader}>
                    <HeaderLeft>
                        {isOpen ? <VscTriangleDown style={{ color }} /> : <VscTriangleRight style={{ color }} />}
                        <Title color={color}>{environment}</Title>
                    </HeaderLeft>
                    <Count color={color}>{formatNumber(count)}</Count>
                </Header>
            }
            body={<Body>Environment children</Body>}
        />
    );
};

export default memo(EnvironmentNode);
