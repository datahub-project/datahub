import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import InfoTooltip from '@app/sharedV2/icons/InfoTooltip';

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
    display: flex;
    gap: 8px;
    font-size: 14px;
    font-weight: 600;
    color: ${ANTD_GRAY[8]};
    min-width: 300px;
`;

interface Props {
    title: string;
    titleInfo?: string;
    chart: React.ReactElement;
    flex?: number;
}

export const ChartCard = ({ title, titleInfo, chart, flex = 1 }: Props) => (
    <Card style={{ flex }}>
        <Header>
            <Heading>
                {title} {titleInfo && <InfoTooltip content={titleInfo} />}
            </Heading>
        </Header>
        <Body>{chart}</Body>
    </Card>
);
