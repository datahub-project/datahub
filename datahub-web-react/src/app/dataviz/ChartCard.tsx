import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

const Card = styled.div`
    display: flex;
    flex-direction: column;
    padding: 1rem;
    background-color: ${(props) => props.theme.colors.bgSurface};
    box-shadow: 0px 3px 6px 0px ${(props) => props.theme.colors.border};
    border-radius: 8px;

    text {
        fill: ${(props) => props.theme.colors.textSecondary};
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
	color: ${(props) => props.theme.colors.textSecondary};
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
