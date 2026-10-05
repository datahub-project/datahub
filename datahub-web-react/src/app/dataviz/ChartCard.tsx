import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import InfoTooltip from '@app/sharedV2/icons/InfoTooltip';

const Card = styled.div`
    display: flex;
    flex-direction: column;
    padding: 1rem;
    background-color: ${(props) => props.theme.colors.bgSurface};
    border: 1px solid ${(props) => props.theme.colors.border};
    box-shadow: ${(props) => props.theme.colors.shadowSm};
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

const Heading = styled(Text)`
    display: flex;
    gap: 8px;
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
            <Heading weight="semiBold">
                {title} {titleInfo && <InfoTooltip content={titleInfo} />}
            </Heading>
        </Header>
        <Body>{chart}</Body>
    </Card>
);
