import { ClockCircleOutlined, LineChartOutlined } from '@ant-design/icons';
import { Button, Typography } from 'antd';
import React from 'react';
import styled, { useTheme } from 'styled-components';

import TabToolbar from '@app/entityV2/shared/components/styled/TabToolbar';
import LookbackWindowSelect from '@app/entityV2/shared/tabs/Dataset/Stats/historical/LookbackWindowSelect';
import { LookbackWindow } from '@app/entityV2/shared/tabs/Dataset/Stats/lookbackWindows';
import { ViewType } from '@app/entityV2/shared/tabs/Dataset/Stats/viewType';

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
    color: ${(props) => props.theme.colors.textTertiary};
`;

type Props = {
    viewType: ViewType;
    setViewType: (type: ViewType) => void;
    reportedAt: string;
    lookbackWindow: LookbackWindow;
    setLookbackWindow: (window: LookbackWindow) => void;
};

export default function StatsHeader({ viewType, setViewType, reportedAt, lookbackWindow, setLookbackWindow }: Props) {
    const theme = useTheme();
    const latestButtonColor = viewType === ViewType.LATEST ? theme.colors.textInformation : theme.colors.textSecondary;
    const latestButton = (
        <Button type="text" onClick={() => setViewType(ViewType.LATEST)}>
            <LineChartOutlined style={{ color: latestButtonColor }} />
            <Typography.Text style={{ color: latestButtonColor }}>Latest</Typography.Text>
        </Button>
    );

    const historicalButtonColor =
        viewType === ViewType.HISTORICAL ? theme.colors.textInformation : theme.colors.textSecondary;
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
