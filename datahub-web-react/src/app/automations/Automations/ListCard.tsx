import React, { useState } from 'react';

import dayjs from 'dayjs';
import localizedFormat from 'dayjs/plugin/localizedFormat';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import StopIcon from '@mui/icons-material/Stop';
import { CustomAvatar } from '../../shared/avatar';
import UndoIcon from '../../../images/undo-icon.svg?react';

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

import { AutomationStatus, truncateString } from '../utils';
import { capitalizeFirstLetter } from '../../shared/textUtil';
import UndoConfirmationModal from './UndoConfirmationModal';

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

    let contentTitle = 'Details';
    if (type === 'Test') contentTitle = 'Results';

    const stopAction = () => {
        return setCurrentStatus(AutomationStatus.STOPPED);
    };

    const runAction = () => {
        return setCurrentStatus(AutomationStatus.RUNNING);
    };

    const undoAction = () => {
        return setShowUndoConfirmation(true);
    };

    // Status States
    const isRunning = currentStatus === AutomationStatus.RUNNING;
    const isStopped = currentStatus === AutomationStatus.STOPPED;
    const showStatus = isRunning || isStopped;

    return (
        <>
            <ListCard>
                <ListCardHeader>
                    <div className="categoryAndDeployed">
                        <Category>{automation.category.toString().toUpperCase()}</Category>
                        <div className="deployedAndStatus">
                            <h4>Deployed: {dayjs(automation.updated).format('L')}</h4>
                            <div className="status">{capitalizeFirstLetter(currentStatus)}</div>
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
                                    <UndoButton onClick={undoAction}>
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
                    setShowUndoConfirmation={setShowUndoConfirmation}
                />
            )}
        </>
    );
};
