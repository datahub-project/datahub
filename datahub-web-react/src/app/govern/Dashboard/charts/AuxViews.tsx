import React from 'react';

import { Skeleton } from 'antd';
import styled from 'styled-components';
import { FcLeave, FcHighPriority, FcMediumPriority } from 'react-icons/fc';

const ChartStateCard = styled.div`
	display: flex;
	align-items: center;
	justify-content: center;
	padding: 0.5rem;
	margin-top: 0.5rem;
	height: 100%;
	width: 100%;
	background-color: #f5f5f5;
	border-radius: 8px;

	svg {
		margin-right: 0.25rem;
	}
`;

// Whole section waiting (waterfall render)
export const SectionWaiting = () =>
	<Skeleton title={false} paragraph={{ 'rows': 1, width: "100%" }} active />;

// Loading, no data, and error states
// TODO: Improve loading state
export const ChartLoading = () =>
	<Skeleton title={false} paragraph={{ 'rows': 1, width: "100%" }} active />

// No data for this time frame
export const ChartNoDataTimeframe = () => (
	<ChartStateCard>
		<FcLeave size={18} />
		No data for this time frame.
	</ChartStateCard>
);

// Not enough data to display helpful information
export const ChartNotEnoughData = () => (
	<ChartStateCard>
		Not enough data to calculate a trend.
	</ChartStateCard>
);

// No data retrieved (just.. nothing)
export const ChartNoData = () => (
	<ChartStateCard>
		<FcMediumPriority size={18} />
		No data received.
	</ChartStateCard>
);

// An error occured
export const ChartError = () => (
	<ChartStateCard>
		<FcHighPriority size={18} />
		An error occured.
	</ChartStateCard>
);

export const ChartState = ({
	loading,
	error,
	noDataTimeframe,
	noData
}: {
	loading: boolean,
	error: boolean,
	noDataTimeframe: boolean,
	noData: boolean
}) => {
	if (!loading && error) return <ChartError />;
	if (!loading && !error && noDataTimeframe) return <ChartNoDataTimeframe />;
	if (!loading && !error && !noDataTimeframe && noData) return <ChartNoData />;
	return <ChartLoading />;
}
