import { LoadingOutlined } from '@ant-design/icons';
import React, { useCallback, useEffect, useState } from 'react';
import styled from 'styled-components';

import StatsSidebarColumnTab from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsSidebarColumnTab';
import { StatsSidebarContent } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsSidebarContent';
import StatsSidebarHeader, {
    StatsViewType,
} from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsSidebarHeader';
import { LOOKBACK_WINDOWS, LookbackWindow } from '@app/entityV2/shared/tabs/Dataset/Stats/lookbackWindows';
import { getFixedLookbackWindow, toLocalDateString, toLocalTimeString } from '@app/shared/time/timeUtils';

import { DatasetFieldProfile, DatasetProfile, SchemaField } from '@types';

export interface StatsProps {
    properties: {
        expandedField: SchemaField;
        fieldProfile: DatasetFieldProfile | undefined;
        profiles: DatasetProfile[];
        fetchDataWithLookbackWindow: (lookbackWindow: any) => void;
        profilesDataLoading: boolean;
    };
}

const LoadingContainer = styled.div`
    margin-top: 50%;
    width: 100%;
    text-align: center;
`;
const LoadingText = styled.div`
    margin-top: 18px;
    font-size: 12px;
`;

const StyledLoading = styled(LoadingOutlined)`
    font-size: 32px;
    color: ${(props) => props.theme.styles['primary-color']};
`;

export default function StatsSidebarView({
    properties: { expandedField, fieldProfile, profiles, fetchDataWithLookbackWindow, profilesDataLoading },
}: StatsProps) {
    const [viewType, setViewType] = useState(StatsViewType.LATEST);
    const [lookbackWindow, setLookbackWindow] = useState(LOOKBACK_WINDOWS.QUARTER);

    /**
     * Handles the change of the lookback window in the UI.
     * Updates the selected lookback window and fetches data with the new window size.
     * @param lookback The new lookback window selected.
     */
    const handleLockbackWindowChange = useCallback(
        (lookback: LookbackWindow) => {
            setLookbackWindow(lookback);
            // Fetch data with the new lookback window size
            fetchDataWithLookbackWindow(getFixedLookbackWindow(lookback.windowSize));
        },
        [fetchDataWithLookbackWindow, setLookbackWindow],
    );

    useEffect(() => {
        handleLockbackWindowChange(LOOKBACK_WINDOWS.QUARTER);
    }, [handleLockbackWindowChange]);

    // Get the latest profile information
    const latestProfile = profiles && profiles[0];
    const reportedAt =
        latestProfile &&
        `Reported on ${toLocalDateString(latestProfile?.timestampMillis)} at ${toLocalTimeString(
            latestProfile?.timestampMillis,
        )}`;

    // Components for insight view and historical stats view
    const insightView = <StatsSidebarContent properties={{ expandedField, fieldProfile, profiles }} />;
    const historicalStats = (
        <StatsSidebarColumnTab
            properties={{ expandedField, fieldProfile, profiles }}
            lookbackWindow={lookbackWindow as LookbackWindow}
        />
    );

    return (
        <>
            {/* Stats sidebar header component */}
            <StatsSidebarHeader
                activeTab={viewType}
                setActiveTab={setViewType}
                lookbackWindow={lookbackWindow}
                setLookbackWindow={handleLockbackWindowChange}
                reportedAt={reportedAt}
            />

            {profilesDataLoading && (
                <LoadingContainer>
                    <StyledLoading />
                    <LoadingText>Loading...</LoadingText>
                </LoadingContainer>
            )}
            {/* Conditional rendering based on active tab */}
            {!profilesDataLoading && (viewType === StatsViewType.HISTORICAL ? historicalStats : insightView)}
        </>
    );
}
