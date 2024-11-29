/* eslint jsx-a11y/anchor-is-valid: 0 */

import React, { useEffect, useState } from 'react';
import { Switch } from 'antd';

import {
    useStopActionPipelineMutation,
    useStartActionPipelineMutation,
    useRollbackActionPipelineMutation,
    useGetActionPipelineStatusQuery,
} from '@graphql/actionPipeline.generated';

import { AutomationStatus, AutomationActionStatus } from '@app/automations/constants';
import { parseJSON, truncateString } from '@app/automations/utils';

import { Description, ListCardHeader, ButtonsContainer, Category, Name, TitleColumn } from '../../components';

import { UndoConfirmationModal } from '../../UndoConfirmationModal';
import { openSuccessNotification, openErrorNotification } from '../../Notifications';

import { ActionsMenu } from '../ActionsMenu';

import { useAutomationContext } from '../../AutomationProvider';
import { useBootstrapActionPipelineMutation } from '../../../../../graphql/actionPipeline.generated';

interface ActionCardProps {
    automation: any;
    openEditModal: () => void;
}

export const ActionCard = ({ automation, openEditModal }: ActionCardProps) => {
    const { urn, details } = automation;
    const { name, category, description } = details;

    const [showUndoConfirmation, setShowUndoConfirmation] = useState(false);
    const [state, setState] = useState<any>(details?.state);

    const [stopActionPipeline] = useStopActionPipelineMutation();
    const [startActionPipeline] = useStartActionPipelineMutation();
    const [rollbackActionPipeline] = useRollbackActionPipelineMutation();
    const [bootstrapActionPipeline] = useBootstrapActionPipelineMutation();

    const { deleteAutomation } = useAutomationContext();

    // TODO: Remove this in favor for returning `status` on `list` query
    const { data, refetch } = useGetActionPipelineStatusQuery({
        skip: !urn,
        fetchPolicy: 'cache-first',
        variables: { urn },
    });

    const fetchedState = data?.actionPipeline?.details?.state;

    // Stop an Action
    const stopAction = () => {
        setState(AutomationStatus.INACTIVE);
        stopActionPipeline({ variables: { urn } })
            .then(() => {
                openSuccessNotification('Stopped automation!');
                refetch();
            })
            .catch((error) => openErrorNotification('Stop Automation', error.message));
    };

    // Start an Action
    const runAction = () => {
        setState(AutomationStatus.ACTIVE);
        startActionPipeline({ variables: { urn } })
            .then(() => {
                openSuccessNotification('Started automation!');
                refetch();
            })
            .catch((error) => openErrorNotification('Start Automation', error.message));
    };

    // Undo an Action
    const undoAction = () => {
        rollbackActionPipeline({ variables: { urn } })
            .then(() => openSuccessNotification('Rollback started!'))
            .catch((error) => openErrorNotification('Rollback Automation', error.message));
        return setShowUndoConfirmation(false);
    };

    // Bootstrap an Action
    const bootstrapAction = () => {
        bootstrapActionPipeline({ variables: { urn } })
            .then(() => openSuccessNotification('Initialization started!'))
            .catch((error) => openErrorNotification('Initialize Automation', error.message));
    };

    // Delete Action
    const deleteAction = () => {
        // Delete is handled by the context
        setState(AutomationStatus.INACTIVE);
        deleteAutomation?.();
    };

    // Set status during poling of refetch
    useEffect(() => {
        if (fetchedState !== undefined && fetchedState !== null) {
            setState(fetchedState);
        }
    }, [fetchedState]);

    // Status States
    const isRunning = state === AutomationStatus.ACTIVE;
    const isStopped = state === AutomationStatus.INACTIVE;

    // Sub Status States
    const status = data?.actionPipeline?.status ? parseJSON(data?.actionPipeline?.status) : undefined;
    const isBootstrapping = status?.bootstrap?.statusCode === AutomationActionStatus.RUNNING;

    return (
        <>
            <ListCardHeader status={state}>
                <TitleColumn>
                    <Category>{category?.toString()?.toUpperCase() || 'Uncategorized'.toUpperCase()}</Category>
                    <Name>{name}</Name>
                </TitleColumn>
                <div className="deployedAndStatus">
                    <Switch checked={isRunning} onChange={!isRunning ? runAction : stopAction} />
                    <ButtonsContainer>
                        <ActionsMenu
                            items={[
                                {
                                    key: 'start',
                                    onClick: runAction,
                                    disabled: isRunning,
                                    hidden: isRunning,
                                    icon: 'PlayCircle',
                                    label: 'Start',
                                    tooltip: 'Start the automation to begin propagating documentation',
                                },
                                {
                                    key: 'stop',
                                    onClick: stopAction,
                                    disabled: isStopped,
                                    hidden: isStopped,
                                    icon: 'PauseCircle',
                                    label: 'Stop',
                                    tooltip: 'Stop the automation from propagating documentation',
                                },
                                {
                                    key: 'bootstrap',
                                    onClick: bootstrapAction,
                                    disabled: isBootstrapping,
                                    icon: 'AutoMode',
                                    label: 'Initialize',
                                    tooltip: 'Backfill the automation for existing data assets. This may take a while!',
                                },
                                // TODO: Rollback is currently disabled due to quality problems.
                                // {
                                //     key: 'undo',
                                //     onClick: () => setShowUndoConfirmation(true),
                                //     disabled: isRunning || isRollbacking,
                                //     icon: 'Restore',
                                //     label: 'Rollback',
                                //     tooltip: `This will rollback all metadata changes made by this automation. ${
                                //         isRunning ? 'Stop the automation before rolling back.' : ''
                                //     }`,
                                // },
                                {
                                    key: 'edit',
                                    onClick: openEditModal,
                                    disabled: isRunning,
                                    icon: 'Edit',
                                    label: 'Edit',
                                    tooltip: `Update the automations configuration. ${
                                        isRunning ? 'Stop the automation before editing.' : ''
                                    }`,
                                },
                                {
                                    key: 'delete',
                                    onClick: deleteAction,
                                    disabled: false,
                                    icon: 'Delete',
                                    label: 'Delete',
                                    tooltip: 'Permanently delete this automation.',
                                },
                            ]}
                        />
                    </ButtonsContainer>
                </div>
            </ListCardHeader>
            {description && <Description className="description">{truncateString(description, 125)}</Description>}
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
