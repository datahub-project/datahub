import React, { useState } from 'react';

import dayjs from 'dayjs';
import localizedFormat from 'dayjs/plugin/localizedFormat';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import StopIcon from '@mui/icons-material/Stop';

import UndoIcon from '../../../images/undo-icon.svg?react';
import { capitalizeFirstLetter } from '../../shared/textUtil';
import { CustomAvatar } from '../../shared/avatar';
import { AutomationStatus, truncateString } from '../utils';

import {
	useStopActionPipelineMutation,
	useStartActionPipelineMutation,
	useRollbackActionPipelineMutation
} from '../../../graphql/actionPipeline.generated';

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
} from './components';

import { UndoConfirmationModal } from './UndoConfirmationModal';

import { openSuccessNotification } from './Notifications';

dayjs.extend(localizedFormat);

const PropagationDetails = () => {
	return <>[propgation details]</>;
};

const MetadataTestDetails = () => {
	return <>[metadata test details]</>;
};

export const AutomationsListCard = ({ automation }: any) => {
	const { type } = automation;

	const [showUndoConfirmation, setShowUndoConfirmation] = useState(false);
	const [currentStatus, setCurrentStatus] = useState<AutomationStatus>(AutomationStatus.STOPPED);

	const [stopActionPipeline] = useStopActionPipelineMutation();
	const [startActionPipeline] = useStartActionPipelineMutation();
	const [rollbackActionPipeline] = useRollbackActionPipelineMutation();

	const contentTitle = type === 'Test' ? 'Results' : 'Details';

	// Stop an Action
	const stopAction = () => {
		stopActionPipeline({ variables: { urn: automation.urn } });
		openSuccessNotification(automation.name, 'Automation stopped successfully');
		return setCurrentStatus(AutomationStatus.STOPPED);
	};

	// Start an Action
	const runAction = () => {
		startActionPipeline({ variables: { urn: automation.urn } });
		openSuccessNotification(automation.name, 'Automation started successfully');
		return setCurrentStatus(AutomationStatus.RUNNING);
	};

	// Undo an Action
	const undoAction = () => {
		rollbackActionPipeline({ variables: { urn: automation.urn } });
		openSuccessNotification(automation.name, 'Undo started successfully');
		return setShowUndoConfirmation(false);
	}

	// Status States
	const isRunning = currentStatus === AutomationStatus.RUNNING;
	const isStopped = currentStatus === AutomationStatus.STOPPED;
	const showStatus = type !== 'Test';

	return (
		<>
			<ListCard>
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
									<UndoButton onClick={() => setShowUndoConfirmation(true)}>
										<UndoIcon />
									</UndoButton>
								)}
							</ButtonsContainer>
						)}
					</div>
				</ListCardHeader>
				<StyledDivider />
				<ListCardBody>
					<div className="createdBy">
						Created by
						<CustomAvatar name="John Doe" />
						xyz
					</div>
					{automation.description && (
						<div className="description">
							<p>{truncateString(automation.description, 125)}</p>
						</div>
					)}
					<div>
						<ContentTitle>{contentTitle}</ContentTitle>
						<Details>
							{type === 'ActionPipeline' && <PropagationDetails />}
							{type === 'Test' && <MetadataTestDetails />}
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
		</>
	);
};
