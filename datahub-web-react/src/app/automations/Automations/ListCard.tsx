import React, { useState } from 'react';

import dayjs from 'dayjs';
import localizedFormat from 'dayjs/plugin/localizedFormat';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import StopIcon from '@mui/icons-material/Stop';
import { Skeleton } from 'antd';

import UndoIcon from '../../../images/undo-icon.svg?react';
import { capitalizeFirstLetter } from '../../shared/textUtil';
// import { CustomAvatar } from '../../shared/avatar';
import { formatNumberWithoutAbbreviation } from '../../shared/formatNumber';
import { toRelativeTimeString } from '../../shared/time/timeUtils';

import {
	useStopActionPipelineMutation,
	useStartActionPipelineMutation,
	useRollbackActionPipelineMutation
} from '../../../graphql/actionPipeline.generated';
import { useGetTestResultsSummaryQuery } from '../../../graphql/test.generated';

import { AutomationStatus, AutomationTypes, truncateString } from '../utils';

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

const PropagationDetails = () => {
	return <>Status summary coming soon.</>;
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
				<Skeleton />
			</div>
			<div style={{ textAlign: 'right' }}>
				<Skeleton />
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

	const [showUndoConfirmation, setShowUndoConfirmation] = useState(false);
	const [currentStatus, setCurrentStatus] = useState<AutomationStatus>(AutomationStatus.STOPPED);
	const [isOpen, setIsOpen] = useState(false);

	const [stopActionPipeline] = useStopActionPipelineMutation();
	const [startActionPipeline] = useStartActionPipelineMutation();
	const [rollbackActionPipeline] = useRollbackActionPipelineMutation();

	const contentTitle = type === AutomationTypes.TEST ? 'Results' : 'Details';

	// Stop an Action
	const stopAction = (e) => {
		e.stopPropagation();
		stopActionPipeline({ variables: { urn: automation.urn } });
		openSuccessNotification(automation.name, 'Automation stopped successfully');
		return setCurrentStatus(AutomationStatus.STOPPED);
	};

	// Start an Action
	const runAction = (e) => {
		e.stopPropagation();
		startActionPipeline({ variables: { urn: automation.urn } });
		openSuccessNotification(automation.name, 'Automation started successfully');
		return setCurrentStatus(AutomationStatus.RUNNING);
	};

	// Undo an Action
	const undoAction = (e) => {
		e.stopPropagation();
		rollbackActionPipeline({ variables: { urn: automation.urn } });
		openSuccessNotification(automation.name, 'Undo started successfully');
		return setShowUndoConfirmation(false);
	}

	// Open Edit Modal
	const openEditModal = (e) => {
		e.stopPropagation();
		setIsOpen(true);
	};

	// Status States
	const isRunning = currentStatus === AutomationStatus.RUNNING;
	const isStopped = currentStatus === AutomationStatus.STOPPED;
	const showStatus = type !== 'Test';

	return (
		<>
			<ListCard onClick={openEditModal}>
				<ListCardHeader>
					<div className="categoryAndDeployed">
						<Category>{automation.category.toString().toUpperCase()}</Category>
						<div className="deployedAndStatus">
							<h4>Deployed: {dayjs(automation.updated).format('L')}</h4>
							{showStatus && <div className="status">{capitalizeFirstLetter(currentStatus)}</div>}
						</div>
					</div>
					<div className="titleAndButtons">
						<h2>{automation.name}</h2>
						{showStatus && (
							<ButtonsContainer>
								{isRunning && (
									<IconContainer onClick={stopAction}>
										<StopIcon />
									</IconContainer>
								)}
								{isStopped && (
									<IconContainer onClick={runAction}>
										<PlayArrowIcon />
									</IconContainer>
								)}
								{isStopped && (
									<UndoButton onClick={(e) => {
										e.stopPropagation();
										setShowUndoConfirmation(true)
									}}>
										<UndoIcon />
									</UndoButton>
								)}
							</ButtonsContainer>
						)}
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
							{type === AutomationTypes.ACTION && <PropagationDetails />}
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
