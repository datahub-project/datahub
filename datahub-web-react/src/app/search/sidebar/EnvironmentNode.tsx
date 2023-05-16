import React, { memo, useCallback, useState } from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { VscTriangleDown, VscTriangleRight } from 'react-icons/vsc';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { formatNumber } from '../../shared/formatNumber';
import ExpandableNode from './ExpandableNode';

const Header = styled.div<{ showHeader: boolean }>`
    display: ${(props) => (props.showHeader ? 'flex' : 'none')};
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
    showHeader: boolean;
};

const EnvironmentNode = ({ environment, count, showHeader }: Props) => {
    const [isSelected, setIsSelected] = useState<boolean>(false);
    const onSelect = useCallback(() => setIsSelected((current) => !current), []);
    const color = ANTD_GRAY[9];

    return (
        <ExpandableNode
            isOpen={!showHeader || isSelected}
            header={
                <Header showHeader={showHeader} onClick={onSelect}>
                    <HeaderLeft>
                        {isSelected ? <VscTriangleDown style={{ color }} /> : <VscTriangleRight style={{ color }} />}
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
