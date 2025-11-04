import { Modal, message } from 'antd';
import { Play, Stop } from 'phosphor-react';
import React from 'react';

import analytics, { EventType } from '@app/analytics';
import { isMonitorActive } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { colors } from '@src/alchemy-components';
import { StructuredPopover } from '@src/alchemy-components/components/StructuredPopover';
import { ActionMenuItem } from '@src/app/entityV2/shared/EntityDropdown/styledComponents';

import { useUpdateMonitorStatusMutation } from '@graphql/monitor.generated';
import { Assertion, Maybe, Monitor, MonitorMode } from '@types';

type Props = {
    assertion: Assertion;
    monitor?: Maybe<Monitor>;
    canEdit: boolean;
    refetch?: () => void;
};

export const StartStopAction = ({ assertion, monitor, canEdit, refetch }: Props) => {
    const [updateMonitorStatusMutation] = useUpdateMonitorStatusMutation();

    if (!monitor) {
        return null;
    }

    const assertionUrn = assertion.urn;
    const monitorUrn = monitor?.urn;

    const onStopMonitor = () => {
        updateMonitorStatusMutation({
            variables: { input: { urn: monitorUrn, mode: MonitorMode.Inactive } },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({ content: 'Stopped!', duration: 2 });
                    refetch?.();
                    analytics.event({
                        type: EventType.StopAssertionMonitorEvent,
                        monitorUrn,
                        assertionUrn,
                        assertionType: assertion.info?.type as string,
                    });
                }
            })
            .catch(() => {
                message.destroy();
                message.error({ content: 'Failed to stop. An unexpected error occurred' });
            });
    };

    const onStartMonitor = () => {
        updateMonitorStatusMutation({
            variables: { input: { urn: monitorUrn, mode: MonitorMode.Active } },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({ content: 'Started!', duration: 2 });
                    refetch?.();
                    analytics.event({
                        type: EventType.StartAssertionMonitorEvent,
                        monitorUrn,
                        assertionUrn,
                        assertionType: assertion.info?.type as string,
                    });
                }
            })
            .catch(() => {
                message.destroy();
                message.error({ content: 'Failed to start. An unexpected error occurred' });
            });
    };

    const onConfirmStartMonitor = () => {
        Modal.confirm({
            title: `Start Monitoring`,
            content: `Are you sure you want to start monitoring this assertion? If you continue, DataHub will begin to issue queries periodically to monitor this table.`,
            onOk() {
                onStartMonitor();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const isActive = isMonitorActive(monitor);
    const authorizedTip = isActive ? 'Stop' : 'Start';
    const unauthorizedTip = canEdit ? undefined : 'You do not have permission to start or stop this assertion';
    return (
        <StructuredPopover title={canEdit ? authorizedTip : unauthorizedTip}>
            <ActionMenuItem
                key="0"
                disabled={!canEdit}
                onClick={isActive ? onStopMonitor : onConfirmStartMonitor}
                icon={
                    isActive ? (
                        <Stop data-testid="assertion-stop-icon" weight="fill" color={colors.violet[500]} />
                    ) : (
                        <Play data-testid="assertion-start-icon" color={colors.violet[500]} />
                    )
                }
                data-testid="assertion-start-stop-action"
            />
        </StructuredPopover>
    );
};
