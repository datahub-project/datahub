import React from 'react';

import { Typography } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../entity/shared/constants';

const Card = styled.div`
    display: flex;
    flex-direction: column;
    padding: 1rem;
    background-color: white;
    box-shadow: 0px 3px 6px 0px ${ANTD_GRAY[5]};
    border-radius: 8px;

    text {
        fill: ${ANTD_GRAY[8]};
        font-weight: 400 !important;
    }
`;

const Header = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
`;

const Body = styled.div`
    position: relative;
    display: flex;
    align-items: center;
    justify-content: center;
`;

const Heading = styled(Typography.Text)`
	display: block;
	font-size: 14px;s
	font-weight: 600;
	color: ${ANTD_GRAY[8]};
	min-width: 300px;
`;

interface Props {
    title: string;
    chart: React.ReactElement;
    flex?: number;
}

export const ChartCard = ({ title, chart, flex = 1 }: Props) => (
    <Card style={{ flex }}>
        <Header>
            <Heading>{title}</Heading>
        </Header>
        <Body>{chart}</Body>
    </Card>
);
