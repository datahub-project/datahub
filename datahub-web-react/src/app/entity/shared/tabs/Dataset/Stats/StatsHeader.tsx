import React from 'react';
import { Button, Typography } from 'antd';
import { ClockCircleOutlined, LineChartOutlined } from '@ant-design/icons';

import styled from 'styled-components';
import { ViewType } from './viewType';
import TabToolbar from '../../../components/styled/TabToolbar';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../../constants';
import { LookbackWindow } from './lookbackWindows';
import LookbackWindowSelect from './historical/LookbackWindowSelect';

const StatsHeaderContainer = styled.div`
    display: flex;
    justify-content: flex-end;
    padding: 0;
    margin: 0;
`;

const ReportedAtLabel = styled.div`
    padding: 0;
    margin: 0;
    display: flex;
    align-items: center;
    color: ${ANTD_GRAY[7]};
`;

type Props = {
    viewType: ViewType;
    setViewType: (type: ViewType) => void;
    reportedAt: string;
    lookbackWindow: LookbackWindow;
    setLookbackWindow: (window: LookbackWindow) => void;
};

export default function StatsHeader({ viewType, setViewType, reportedAt, lookbackWindow, setLookbackWindow }: Props) {
    const latestButtonColor = viewType === ViewType.LATEST ? REDESIGN_COLORS.BLUE : ANTD_GRAY[8];
    const latestButton = (
        <Button type="text" onClick={() => setViewType(ViewType.LATEST)}>
            <LineChartOutlined style={{ color: latestButtonColor }} />
            <Typography.Text style={{ color: latestButtonColor }}>Latest</Typography.Text>
        </Button>
    );

    const historicalButtonColor = viewType === ViewType.HISTORICAL ? REDESIGN_COLORS.BLUE : ANTD_GRAY[8];
    const historicalButton = (
        <Button type="text" onClick={() => setViewType(ViewType.HISTORICAL)}>
            <ClockCircleOutlined style={{ color: historicalButtonColor }} />
            <Typography.Text style={{ color: historicalButtonColor }}>Historical</Typography.Text>
        </Button>
    );

    const actionView =
        viewType === ViewType.HISTORICAL ? (
            <LookbackWindowSelect lookbackWindow={lookbackWindow} setLookbackWindow={setLookbackWindow} />
        ) : (
            <ReportedAtLabel>{reportedAt}</ReportedAtLabel>
        );

    return (
        <TabToolbar>
            <StatsHeaderContainer>
                {latestButton}
                {historicalButton}
            </StatsHeaderContainer>
            {actionView}
        </TabToolbar>
    );
}
