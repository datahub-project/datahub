/* eslint jsx-a11y/anchor-is-valid: 0 */

import React, { useEffect, useState } from 'react';
import { Switch } from 'antd';

import { Heading, Text } from '@components';
import { toLocalDateTimeString, toRelativeTimeString } from '@app/shared/time/timeUtils';

import {
    useStopActionPipelineMutation,
    useStartActionPipelineMutation,
    useRollbackActionPipelineMutation,
    useGetActionPipelineStatusQuery,
} from '@graphql/actionPipeline.generated';

import { env, AutomationStatus, AutomationActionStatus } from '@app/automations/constants';
import { parseJSON, truncateString } from '@app/automations/utils';

import {
    Description,
    ListCardHeader,
    ListCardBody,
    ButtonsContainer,
    StyledDivider,
    Category,
    Name,
    Details,
    ResultContainer,
    ResultGroup,
    TitleColumn,
} from '../../components';

import { UndoConfirmationModal } from '../../UndoConfirmationModal';
import { openSuccessNotification } from '../../Notifications';

import { ActionsMenu } from '../ActionsMenu';

import { useAutomationContext } from '../../AutomationProvider';
import { useBootstrapActionPipelineMutation } from '../../../../../graphql/actionPipeline.generated';

const ActonDetails = ({ status, state }: any) => {
    if (!state || !status) {
        return (
            <ResultContainer>
                <div>
                    <Heading size="md" weight="bold">
                        Performance
                    </Heading>
                    <Text size="sm" color="black">
                        Status unavailable.
                    </Text>
                </div>
            </ResultContainer>
        );
    }

    // Status from server
    const { live, bootstrap, rollback } = status;

    // Map to cleaner object
    const rollbackStatus = {
        totalAssetsProcessed: rollback?.customProperties?.total_assets_processed,
        totalAssetsToProcess: rollback?.customProperties?.total_assets_to_process,
        lastEventProcessTime: toLocalDateTimeString(
            rollback?.customProperties?.eventProcessingStats?.last_event_processed_time,
        ),
        lastEventProcessTimeRelative: toRelativeTimeString(
            rollback?.customProperties?.eventProcessingStats?.last_event_processed_time,
        ),
        endTime: toLocalDateTimeString(rollback?.endTime),
        endTimeRelative: toRelativeTimeString(rollback?.endTime),
    };

    // Map to cleaner object
    const bootstrapStatus = {
        totalAssetsProcessed: bootstrap?.customProperties?.total_assets_processed,
        totalAssetsToProcess: bootstrap?.customProperties?.total_assets_to_process,
        lastEventProcessTime: toLocalDateTimeString(
            bootstrap?.customProperties?.eventProcessingStats?.last_event_processed_time,
        ),
        lastEventProcessTimeRelative: toRelativeTimeString(
            bootstrap?.customProperties?.eventProcessingStats?.last_event_processed_time,
        ),
        endTime: toLocalDateTimeString(bootstrap?.endTime),
        endTimeRelative: toRelativeTimeString(bootstrap?.endTime),
    };

    // Map to cleaner object
    const liveStatus = {
        totalAssetsImpacted: live?.customProperties?.total_assets_impacted,
        totalAssetsProcessed: live?.customProperties?.total_assets_processed,
        totalActionsExecuted: live?.customProperties?.total_actions_executed,
        startTime: toLocalDateTimeString(live?.startTime),
        startTimeRelative: toRelativeTimeString(live?.startTime),
    };

    // Return the component
    return (
        <ResultContainer>
            <div>
                <Heading size="md" weight="bold">
                    Performance
                </Heading>
                {bootstrap && (
                    <>
                        {bootstrap.statusCode === AutomationActionStatus.RUNNING && (
                            <ResultGroup>
                                <Text size="sm" color="black">
                                    Scanning ― EST.{' '}
                                    <strong>
                                        {bootstrapStatus.totalAssetsToProcess} Assets <br /> Last Event Processed{' '}
                                        {bootstrapStatus.lastEventProcessTime} (
                                        {bootstrapStatus.lastEventProcessTimeRelative})
                                    </strong>
                                </Text>
                                {/* <Text size="sm">Next scan scheduled: 1 week</Text> */}
                            </ResultGroup>
                        )}
                        {bootstrap.statusCode === AutomationActionStatus.SUCCEEDED && (
                            <ResultGroup>
                                <Text size="sm" color="black">
                                    Scanned ― {bootstrapStatus.totalAssetsProcessed} of{' '}
                                    {bootstrap?.customProperties?.totalAssetsToProcess} Assets <br /> Completed{' '}
                                    {bootstrapStatus.endTime} ({bootstrapStatus.endTimeRelative})
                                </Text>
                                {/* <Text size="sm">Next scan scheduled: 1 week</Text> */}
                            </ResultGroup>
                        )}
                    </>
                )}
                {rollback && (
                    <>
                        {rollback.statusCode === AutomationActionStatus.RUNNING && (
                            <ResultGroup>
                                <Text size="sm" color="black">
                                    <strong>Rollback ― </strong> {rollbackStatus.totalAssetsProcessed} Asset Processed ·{' '}
                                    {rollbackStatus.totalAssetsToProcess} Assets to Process <br /> Last Event Processed{' '}
                                    {rollbackStatus.lastEventProcessTime} ({rollbackStatus.lastEventProcessTimeRelative}
                                    )
                                </Text>
                            </ResultGroup>
                        )}
                        {rollback.statusCode === AutomationActionStatus.SUCCEEDED && (
                            <ResultGroup>
                                <Text size="sm" color="black">
                                    <strong>Rollback ― </strong> {rollbackStatus.totalAssetsProcessed} Asset Processed ·{' '}
                                    {rollbackStatus.totalAssetsToProcess} Assets to Process <br /> Completed{' '}
                                    {rollbackStatus.endTime} ({rollbackStatus.endTimeRelative})
                                </Text>
                            </ResultGroup>
                        )}
                    </>
                )}
                {live && live.statusCode === AutomationActionStatus.RUNNING && state !== AutomationStatus.INACTIVE && (
                    <ResultGroup>
                        <Text size="sm" color="black">
                            <strong>Live Processing ―</strong> {liveStatus.totalAssetsImpacted} Assets Impacted ·{' '}
                            {liveStatus.totalAssetsProcessed} Assets Processed · {liveStatus.totalActionsExecuted}{' '}
                            Actions Executed <br /> Running Since {liveStatus.startTime} ({liveStatus.startTimeRelative}
                            )
                        </Text>
                    </ResultGroup>
                )}
                {!live && live.statusCode === AutomationActionStatus.RUNNING && (
                    <ResultGroup>
                        <Text size="sm" color="black">
                            <strong>Live Processing ―</strong> Fetching status…
                        </Text>
                    </ResultGroup>
                )}
            </div>
        </ResultContainer>
    );
};

interface ActionCardProps {
    automation: any;
    openEditModal: () => void;
}

export const ActionCard = ({ automation, openEditModal }: ActionCardProps) => {
    const { actionStatusPollingInterval, hideActionStatus } = env;
    const { urn, category, description, name } = automation;

    const [showUndoConfirmation, setShowUndoConfirmation] = useState(false);
    const [status, setStatus] = useState<any>();
    const [state, setState] = useState<any>();

    const [stopActionPipeline] = useStopActionPipelineMutation();
    const [startActionPipeline] = useStartActionPipelineMutation();
    const [rollbackActionPipeline] = useRollbackActionPipelineMutation();
    const [bootstrapActionPipeline] = useBootstrapActionPipelineMutation();

    const { deleteAutomation } = useAutomationContext();

    // TODO: Remove this in favor for returning `status` on `list` query
    const { data, refetch: refetchStatus } = useGetActionPipelineStatusQuery({
        skip: !urn,
        pollInterval: actionStatusPollingInterval, // 1 minute
        fetchPolicy: 'no-cache',
        variables: { urn },
    });

    const { details, status: rawStatus } = data?.actionPipeline || {};

    // Stop an Action
    const stopAction = () => {
        setState(AutomationStatus.INACTIVE);
        stopActionPipeline({ variables: { urn: automation.urn } });
        openSuccessNotification('Stopped automation!');
        refetchStatus();
    };

    // Start an Action
    const runAction = () => {
        setState(AutomationStatus.ACTIVE);
        startActionPipeline({ variables: { urn: automation.urn } });
        openSuccessNotification('Started automation!');
        refetchStatus();
    };

    // Undo an Action
    const undoAction = () => {
        rollbackActionPipeline({ variables: { urn: automation.urn } });
        openSuccessNotification('Rollback started!');
        refetchStatus();
        return setShowUndoConfirmation(false);
    };

    // Bootstrap an Action
    const bootstrapAction = () => {
        bootstrapActionPipeline({ variables: { urn: automation.urn } });
        openSuccessNotification('Bootstrap started!');
        refetchStatus();
    };

    // Delete Action
    const deleteAction = () => {
        // Delete is handled by the context
        setState(AutomationStatus.INACTIVE);
        deleteAutomation?.();
        openSuccessNotification('Deleted automation!');
    };

    // Set status during poling of refetch
    useEffect(() => {
        if (rawStatus) setStatus(parseJSON(rawStatus));
        else setStatus(null);
        if (details?.state) setState(details?.state);
        else setState(null);
    }, [rawStatus, details]);

    // Status States
    const isRunning = state === AutomationStatus.ACTIVE;
    const isStopped = state === AutomationStatus.INACTIVE;

    // Sub Status States
    // const isRollbacking = status?.rollback?.statusCode === AutomationActionStatus.RUNNING;
    const isBootstrapping = status?.bootstrap?.statusCode === AutomationActionStatus.RUNNING;

    return (
        <>
            <ListCardHeader status={state}>
                <TitleColumn>
                    <Category>{category.toString().toUpperCase()}</Category>
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
            {!hideActionStatus && <StyledDivider />}
            <ListCardBody>
                {!hideActionStatus && (
                    <Details>
                        <ActonDetails status={status} state={state} />
                    </Details>
                )}
            </ListCardBody>
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
