import React from 'react';

import styled from 'styled-components';
import { Modal, message } from 'antd';
import { CaretRightOutlined } from '@ant-design/icons';

import analytics, { EventType } from '../../../../../../../../analytics';
import { ActionItem } from './ActionItem';
import { useUpdateMonitorStatusMutation } from '../../../../../../../../../graphql/monitor.generated';
import { Assertion, Monitor, MonitorMode } from '../../../../../../../../../types.generated';
import { isMonitorActive } from '../../../acrylUtils';

const StyledStopOutlined = styled.div`
    && {
        display: flex;
        background-color: #fff;
        width: 8px;
        height: 8px;
    }
`;

const StyledCaretOutlined = styled(CaretRightOutlined)`
    && {
        font-size: 16px;
        display: flex;
        padding-left: 2px;
    }
`;

type Props = {
    assertion: Assertion;
    monitor?: Monitor;
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
        <ActionItem
            key="0"
            tip={canEdit ? authorizedTip : unauthorizedTip}
            disabled={!canEdit}
            onClick={isActive ? onStopMonitor : onConfirmStartMonitor}
            icon={isActive ? <StyledStopOutlined /> : <StyledCaretOutlined />}
            dataTestId="assertion-start-stop-action"
        />
    );
};
