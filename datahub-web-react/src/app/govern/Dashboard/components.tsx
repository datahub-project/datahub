import { Typography } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../entity/shared/constants';

export const Layout = styled.div`
	flex: 1;
	display: flex;
	flex-direction: column;
`;

export const Header = styled.div`
	display: flex;
	height: 85px;
	align-items: center;
	justify-content: space-between;
	padding: 0 1rem;
`;

export const TabsContainer = styled.div`
	display: flex;
	align-items: flex-end;
	justify-content: space-between;
	height: 40px;
	border-bottom: 1px solid #E8EBED;
	padding: 0 1rem;

	.ant-tabs {
		margin-bottom: -17px;
	}

	.ant-tabs-tab {
		font-size: 16px;
	}
`;

export const SeriesContainer = styled.div`
	display: flex;
	align-items: center;
	margin-right: 1rem;
`;

export const SeriesButtons = styled.div`
	display: flex;
	align-items: center;

	button {
		box-shadow: none;
		margin-left: -1px;
		font-weight: 400;
		font-size: 12px;

		&:first-child {
			border-top-right-radius: 0;
			border-bottom-right-radius: 0;
		}

		&:not(:first-child):not(:last-child) {
			border-radius: 0;
		}

		&:last-child {
			border-top-left-radius: 0;
			border-bottom-left-radius: 0;
		}
	}
`;

export const SeriesLabel = styled.div`
	font-size: 10px;
	margin-right: 0.5rem;
	opacity: 0.75;
`;

export const BodyHeader = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
	margin-bottom: 1rem;
	height: 40px;

	svg {
		opacity: 0.25;

		&:hover {
			opacity: 0.75;
		}
	}
`;

export const DataFreshness = styled.div`
	margin-bottom: 11px;

	span {
		display: flex;
		align-items: center;

		&:hover {
			cursor: default;
		}
	}

	svg {
		color: orange;
		margin-right: 0.15rem;
	}
`;

export const Filters = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
`;

export const Body = styled.div`
	flex: 1;
	display: flex;
	flex-direction: column;
	background-color: #F8F9FA;
	padding: 1rem;
`;

export const ChartGroup = styled.div`
	width: 100%;
	margin-bottom: 2rem;
`;

export const Row = styled.div`
	display: flex;
	gap: 1rem;
	width: 100%;
`;

export const PrimaryHeading = styled(Typography.Text)`
	font-size: 24px;
	font-weight: 600;
`;

export const SecondaryHeading = styled(Typography.Text)`
	display: block;
	font-size: 18px;
	font-weight: 600;
	margin-bottom: 0.5rem;
	color: inherit;
`;

export const StatusSeriesWrapper = styled.div`
	width: 100%;
`;

export const StatusSeriesHeading = styled(Typography.Text)`
	display: block;
	font-size: 18px;
	font-weight: 600;
	margin-top: 0.5rem;
	color: #00615F;
`;

export const StatusSeriesDescription = styled(Typography.Text)`
	display: block;
	font-size: 12px;
	font-weight: 400;
	color: ${ANTD_GRAY[7]};
	max-width: 85%;
`;

export const ChartPerformanceItems = styled.div`
	width: 100%;
`;

export const ChartPerformanceItem = styled.div`
	display: flex;
	align-items: center;
	justify-content: space-between;
	width: 100%;
	margin-top: 1rem;
`;