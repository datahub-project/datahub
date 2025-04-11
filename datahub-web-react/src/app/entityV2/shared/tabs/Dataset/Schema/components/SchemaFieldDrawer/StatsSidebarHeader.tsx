import React from 'react';
import { ClockCircleOutlined, LineChartOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import LookbackWindowSelect from '../../../Stats/historical/LookbackWindowSelect';
import { LookbackWindow } from '../../../Stats/lookbackWindows';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../../../../constants';

export enum StatsViewType {
    LATEST,
    HISTORICAL,
}

const TabContainer = styled.div`
    display: flex;
    margin: 15px 0px 0px;
    align-items: start;
    padding: 0 15px;
    gap: 20px;
    flex-direction: column;
`;

const StatsTabViewSwitch = styled.div<{ isActive: boolean }>`
    background: ${({ isActive }) => (isActive ? `${REDESIGN_COLORS.BACKGROUND_PRIMARY_1}` : 'transperent')};
    border-radius: 4px;
    color: ${({ isActive }) => (isActive ? '#fff' : '#56668E')};
    cursor: pointer;
    display: flex;
    font-size: 14px;
    justify-content: center;
    line-height: 30px;
    height: 42px;
    width: 180px;
    align-items: center;
    gap: 5px;
    transition: background 0.3s ease, color 0.3s ease;
`;

const SwitchWrapper = styled.div`
    border-radius: 4.5px;
    display: flex;
    width: 250px;
    width: 100%;
    background: ${REDESIGN_COLORS.BACKGROUND_GRAY_3};
`;

const ReportedAtLabel = styled.div`
    padding: 0;
    margin: 0;
    display: flex;
    align-items: center;
    color: ${ANTD_GRAY[7]};
`;

const ActionContainer = styled.div`
    display: flex;
    justify-content: end;
    width: 100%;
    span {
        font-size: 12px;
    }
`;

type Props = {
    activeTab: StatsViewType;
    setActiveTab: (type: StatsViewType) => void;
    reportedAt: string;
    lookbackWindow: LookbackWindow;
    setLookbackWindow: (window: LookbackWindow) => void;
};

export default function StatsSidebarHeader({
    activeTab,
    setActiveTab,
    lookbackWindow,
    setLookbackWindow,
    reportedAt,
}: Props) {
    const handleTabClick = (type: StatsViewType) => {
        setActiveTab(type);
    };

    const actionView =
        activeTab === StatsViewType.HISTORICAL ? (
            <LookbackWindowSelect lookbackWindow={lookbackWindow} setLookbackWindow={setLookbackWindow} />
        ) : (
            <ReportedAtLabel>{reportedAt}</ReportedAtLabel>
        );

    return (
        <TabContainer>
            <SwitchWrapper>
                <StatsTabViewSwitch
                    isActive={activeTab === StatsViewType.LATEST}
                    onClick={() => handleTabClick(StatsViewType.LATEST)}
                >
                    <LineChartOutlined />
                    Stats & Insights
                </StatsTabViewSwitch>
                <StatsTabViewSwitch
                    isActive={activeTab === StatsViewType.HISTORICAL}
                    onClick={() => handleTabClick(StatsViewType.HISTORICAL)}
                >
                    <ClockCircleOutlined />
                    Historical Stats
                </StatsTabViewSwitch>
            </SwitchWrapper>
            <ActionContainer>{actionView}</ActionContainer>
        </TabContainer>
    );
}
