import React, { useEffect, useState, useRef } from 'react';

import dayjs from 'dayjs';
import localizedFormat from 'dayjs/plugin/localizedFormat';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import PauseIcon from '@mui/icons-material/Pause';
import { LoadingOutlined } from '@ant-design/icons';
import { Tooltip } from 'antd';

import UndoIcon from '../../../images/undo-icon.svg?react';
import { capitalizeFirstLetter } from '../../shared/textUtil';
// import { CustomAvatar } from '../../shared/avatar';
import { formatNumberWithoutAbbreviation } from '../../shared/formatNumber';
import { toRelativeTimeString } from '../../shared/time/timeUtils';

import {
	useStopActionPipelineMutation,
	useStartActionPipelineMutation,
	useRollbackActionPipelineMutation,
	useGetActionPipelineStatusQuery
} from '../../../graphql/actionPipeline.generated';
import { useGetTestResultsSummaryQuery } from '../../../graphql/test.generated';

import { AutomationStatus, AutomationTypes, parseJSON, truncateString } from '../utils';

import {
	ListCard,
	ListCardHeader,
	ListCardBody,
	ButtonsContainer,
	IconContainer,
	UndoButton,
	StyledDivider,
	Category,
	ContentTitle,
	Details,
	ResultContainer,
} from './components';

import { AutomationModal } from './Modal';
import { UndoConfirmationModal } from './UndoConfirmationModal';
import { openSuccessNotification } from './Notifications';

dayjs.extend(localizedFormat);

const Loading = ({ ...props }: any) =>
	<LoadingOutlined
		style={{ fontSize: 8, marginRight: '4px', ...props.style }}
		spin
		{...props}
	/>

const PropagationDetails = ({ status, statusLoading }: any) => {
	// TODO: Come up with an empty state for no status
	if (!status || status === null || !status.main) {
		if (statusLoading) {
			return (
				<ResultContainer>
					<div>
						Assets updated: &nbsp;
						<Loading />
					</div>
					<div style={{ textAlign: 'right' }}>
						Started &nbsp;
						<Loading />
					</div>
				</ResultContainer>
			);
		}

		return (
			<ResultContainer>
				<div>
					Assets updated: N/A
				</div>
				<div style={{ textAlign: 'right' }}>
					Not running
				</div>
			</ResultContainer>
		);
	}

	/* eslint-disable @typescript-eslint/naming-convention */
	const { main: { started_at, success_count } } = status;

	const startedAt = started_at !== undefined
		? toRelativeTimeString(started_at || 0)
		: 'unknown';

	const successCount = success_count !== undefined
		? formatNumberWithoutAbbreviation(success_count)
		: 0;

	return (
		<ResultContainer>
			<div>
				<span>
					Assets updated: {successCount}
				</span>
			</div>
			<div style={{ textAlign: 'right' }}>
				Started {startedAt}
			</div>
		</ResultContainer>
	);
};

const MetadataTestDetails = ({ urn }: any) => {
	const { data: results, loading } = useGetTestResultsSummaryQuery({
		skip: !urn,
		variables: {
			urn,
		},
	});

	const passingCount =
		results?.test?.results?.passingCount !== undefined
			? formatNumberWithoutAbbreviation(results?.test?.results?.passingCount)
			: '-';

	const failingCount =
		results?.test?.results?.failingCount !== undefined
			? formatNumberWithoutAbbreviation(results?.test?.results?.failingCount)
			: '-';

	const lastComputed = results?.test?.results?.lastRunTimestampMillis !== undefined
		? toRelativeTimeString(results?.test?.results?.lastRunTimestampMillis || 0)
		: 'unknown';

	if (loading) {
		<ResultContainer>
			<div>
				<span className="pass">Passing: &nbsp; <Loading /></span> <br />
				<span className="fail">Failing: &nbsp; <Loading /></span> <br />
			</div>
			<div style={{ textAlign: 'right' }}>
				Last Computed <br />
				<Loading />
			</div>
		</ResultContainer>
	}

	return (
		<ResultContainer>
			<div>
				<span className="pass">Passing: {passingCount}</span> <br />
				<span className="fail">Failing: {failingCount}</span> <br />
			</div>
			<div style={{ textAlign: 'right' }}>
				Last Computed <br />
				{lastComputed}
			</div>
		</ResultContainer>
	);
};

export const AutomationsListCard = ({ automation }: any) => {
	const { type } = automation;
	const statusRefetchInterval = useRef<any>();
	const refetchMiliseconds = 2000;

	const [showUndoConfirmation, setShowUndoConfirmation] = useState(false);
	const [currentStatus, setCurrentStatus] = useState<AutomationStatus | undefined>();
	const [status, setStatus] = useState<any>();
	const [isOpen, setIsOpen] = useState(false);

	const [stopActionPipeline] = useStopActionPipelineMutation();
	const [startActionPipeline] = useStartActionPipelineMutation();
	const [rollbackActionPipeline] = useRollbackActionPipelineMutation();

	// TODO: Remove this in favor for returning `status` on `list` query
	const { data, loading, refetch } = useGetActionPipelineStatusQuery({
		skip: type !== AutomationTypes.ACTION || !automation.urn,
		variables: {
			urn: automation.urn,
		},
	});

	const refetchStatus = () => {
		console.log('Refetching status');
		console.log('Current status', currentStatus);
		return refetch();
	}

	const resetInterval = (interval) => {
		clearInterval(statusRefetchInterval.current);
		statusRefetchInterval.current = interval;
	}

	const { status: rawStatus } = data?.actionPipeline || {};

	const contentTitle = type === AutomationTypes.TEST ? 'Results' : 'Details';

	// Stop an Action
	const stopAction = (e) => {
		e.stopPropagation();
		stopActionPipeline({ variables: { urn: automation.urn } });
		openSuccessNotification(automation.name, 'Automation stopped successfully');

		// Refetch status & clear poll
		setStatus(null);
		resetInterval(undefined);
		setCurrentStatus(AutomationStatus.STOPPED);
	};

	// Start an Action
	const runAction = (e) => {
		e.stopPropagation();
		startActionPipeline({ variables: { urn: automation.urn } });
		openSuccessNotification(automation.name, 'Automation started successfully');

		// Start polling for status
		refetchStatus();
		resetInterval(setInterval(refetchStatus, refetchMiliseconds));
		setCurrentStatus(AutomationStatus.RUNNING);
	};

	// Undo an Action
	const undoAction = (e) => {
		e.stopPropagation();
		rollbackActionPipeline({ variables: { urn: automation.urn } });
		openSuccessNotification(automation.name, 'Undo started successfully');

		// Start polling for status
		refetchStatus();
		resetInterval(setInterval(refetchStatus, refetchMiliseconds));

		return setShowUndoConfirmation(false);
	}

	// Open Edit Modal
	const openEditModal = (e) => {
		e.stopPropagation();
		setIsOpen(true);
	};

	// Set status during poling of refetch
	useEffect(() => {
		if (rawStatus)
			setStatus(parseJSON(rawStatus));
		else
			setStatus(null);
	}, [rawStatus]);

	// Set status state based on the data
	useEffect(() => {
		if (data) {
			if (!status || status === null || !status.main)
				setCurrentStatus(AutomationStatus.STOPPED);
			else
				setCurrentStatus(AutomationStatus.RUNNING);
		}
	}, [data, status]);

	// Cleanup interval on component unmount
	useEffect(() => {
		return () => {
			clearInterval(statusRefetchInterval.current);
		};
	}, []);

	// Status States
	const isRunning = currentStatus === AutomationStatus.RUNNING;
	const isStopped = currentStatus === AutomationStatus.STOPPED;
	const showStatus = type === AutomationTypes.ACTION && (isRunning || isStopped);

	return (
		<>
			<ListCard onClick={openEditModal}>
				<ListCardHeader status={currentStatus}>
					<div className="categoryAndDeployed">
						<Category>{automation.category.toString().toUpperCase()}</Category>
						<div className="deployedAndStatus">
							{/* <h4>Deployed: {dayjs(automation.updated).format('L')}</h4> */}
							{showStatus && (
								<div className="status">
									{currentStatus === AutomationStatus.RUNNING && (
										<LoadingOutlined
											style={{ fontSize: 8, marginRight: '4px', color: '#845B10' }}
											spin
										/>
									)}
									{capitalizeFirstLetter(currentStatus)}
								</div>
							)}
							{showStatus && (
								<ButtonsContainer>
									{isRunning && (
										<Tooltip title="Stop Automation">
											<IconContainer onClick={stopAction}>
												<PauseIcon />
											</IconContainer>
										</Tooltip>
									)}
									{isStopped && (
										<Tooltip title="Start Automation">
											<IconContainer onClick={runAction}>
												<PlayArrowIcon />
											</IconContainer>
										</Tooltip>
									)}
									{isStopped && (
										<Tooltip title="Undo Automation Changes">
											<UndoButton onClick={(e) => {
												e.stopPropagation();
												setShowUndoConfirmation(true)
											}}>
												<UndoIcon />
											</UndoButton>
										</Tooltip>
									)}
								</ButtonsContainer>
							)}
						</div>
					</div>
					<div className="titleAndButtons">
						<h2>{automation.name}</h2>
					</div>
				</ListCardHeader>
				<StyledDivider />
				<ListCardBody>
					{/* <div className="createdBy">
						Created by
						<CustomAvatar name="John Doe" />
						xyz
					</div> */}
					{automation.description && (
						<div className="description">
							<p>{truncateString(automation.description, 125)}</p>
						</div>
					)}
					<div>
						<ContentTitle>{contentTitle}</ContentTitle>
						<Details>
							{type === AutomationTypes.ACTION && <PropagationDetails status={status} statusLoading={loading} />}
							{type === AutomationTypes.TEST && <MetadataTestDetails urn={automation.urn} />}
						</Details>
					</div>
				</ListCardBody>
			</ListCard>
			{showUndoConfirmation && (
				<UndoConfirmationModal
					showUndoConfirmation={showUndoConfirmation}
					handleClose={() => setShowUndoConfirmation(false)}
					handleUndo={undoAction}
				/>
			)}
			<AutomationModal
				isOpen={isOpen}
				setIsOpen={setIsOpen}
				type="EDIT"
				data={automation}
			/>
		</>
	);
};
