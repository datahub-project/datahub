import React, { useState } from 'react';
import { message, Modal } from 'antd';
import { Assertion, Monitor, MonitorMode } from '../../../../../../types.generated';
import { useDeleteAssertionMutation } from '../../../../../../graphql/assertion.generated';
import { useUpdateMonitorStatusMutation, useDeleteMonitorMutation } from '../../../../../../graphql/monitor.generated';
import { AssertionActionsBuilderModal } from './assertion/builder/AssertionActionsBuilderModal';
import { AcrylAssertionsTable } from './AcrylAssertionsTable';
import analytics, { EventType } from '../../../../../analytics';

type Props = {
    assertions: Array<Assertion>;
    onDeletedAssertion?: (urn: string) => void;
    onUpdatedAssertion?: (assertion: Assertion) => void;
};

/**
 * Acryl-specific list of assertions displaying their most recent run status, their human-readable
 * description, and platform.
 *
 * Currently this component supports rendering Dataset Assertions only.
 */
export const AcrylDatasetAssertionsList = ({ assertions, onDeletedAssertion, onUpdatedAssertion }: Props) => {
    const [deleteAssertionMutation] = useDeleteAssertionMutation();
    const [deleteMonitorMutation] = useDeleteMonitorMutation();

    const [updateMonitorStatusMutation] = useUpdateMonitorStatusMutation();
    const [managingAssertion, setManagingAssertion] = useState<Assertion | undefined>(undefined);

    const deleteMonitor = async (urn: string) => {
        try {
            await deleteMonitorMutation({
                variables: { urn },
            });
            await message.success({ content: 'Removed assertion.', duration: 2 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({
                    content: `Failed to remove assertion monitor. An unknown error occurred`,
                    duration: 3,
                });
            }
        }
    };

    const deleteAssertion = async (urn: string) => {
        const deletedAssertion = assertions.find((assertion) => assertion.urn === urn);
        const monitorUrn =
            (deletedAssertion as any)?.monitor?.relationships?.length &&
            (deletedAssertion as any).monitor.relationships[0].entity?.urn;
        try {
            await deleteAssertionMutation({
                variables: { urn },
            });
            if (monitorUrn) {
                deleteMonitor(monitorUrn);
            } else {
                await message.success({ content: 'Removed assertion.', duration: 2 });
            }
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to remove assertion. An unknown error occurred`, duration: 3 });
            }
        }
        onDeletedAssertion?.(urn);
    };

    const onDeleteAssertion = (urn: string) => {
        Modal.confirm({
            title: `Confirm Assertion Removal`,
            content: `Are you sure you want to remove this assertion from the dataset?`,
            onOk() {
                deleteAssertion(urn);
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const onManageAssertion = (urn: string) => {
        setManagingAssertion(assertions.find((assertion) => assertion.urn === urn));
    };

    const onUpdateAssertion = (assertion: Assertion) => {
        onUpdatedAssertion?.(assertion);
        setManagingAssertion(undefined);
    };

    const onStartMonitor = (assertionUrn: string, monitorUrn: string) => {
        updateMonitorStatusMutation({
            variables: { input: { urn: monitorUrn, mode: MonitorMode.Active } },
        })
            .then(({ data, errors }) => {
                if (!errors) {
                    message.success({ content: 'Started assertion.', duration: 2 });
                    const updatedAssertion = assertions.find((assertion) => assertion.urn === assertionUrn);
                    onUpdatedAssertion?.({
                        ...updatedAssertion,
                        monitor: {
                            relationships: [
                                {
                                    entity: data?.updateMonitorStatus as Monitor,
                                },
                            ],
                        },
                    } as Assertion);
                    analytics.event({
                        type: EventType.StartAssertionMonitorEvent,
                        monitorUrn,
                        assertionUrn,
                        assertionType: updatedAssertion?.info?.type as string,
                    });
                }
            })
            .catch(() => {
                message.destroy();
                message.error({ content: 'Failed to start Assertion. An unexpected error occurred' });
            });
    };

    const onStopMonitor = (assertionUrn: string, monitorUrn: string) => {
        updateMonitorStatusMutation({
            variables: { input: { urn: monitorUrn, mode: MonitorMode.Inactive } },
        })
            .then(({ data, errors }) => {
                if (!errors) {
                    message.success({ content: 'Stopped assertion.', duration: 2 });
                    const updatedAssertion = assertions.find((assertion) => assertion.urn === assertionUrn);
                    onUpdateAssertion?.({
                        ...updatedAssertion,
                        monitor: {
                            total: 1,
                            start: 0,
                            count: 1,
                            relationships: [
                                {
                                    entity: data?.updateMonitorStatus as Monitor,
                                },
                            ],
                        },
                    } as Assertion);
                    analytics.event({
                        type: EventType.StopAssertionMonitorEvent,
                        monitorUrn,
                        assertionUrn,
                        assertionType: updatedAssertion?.info?.type as string,
                    });
                }
            })
            .catch(() => {
                message.destroy();
                message.error({ content: 'Failed to stop Assertion. An unexpected error occurred' });
            });
    };

    const onConfirmStartMonitor = (assertionUrn: string, monitorUrn: string) => {
        Modal.confirm({
            title: `Start Assertion Monitoring`,
            content: `Are you sure you want to start monitoring this Assertion? If you continue, DataHub will begin to issue queries periodically to monitor this table.`,
            onOk() {
                onStartMonitor(assertionUrn, monitorUrn);
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    return (
        <>
            <AcrylAssertionsTable
                assertions={assertions}
                onManageAssertion={onManageAssertion}
                onDeleteAssertion={onDeleteAssertion}
                onStartMonitor={onConfirmStartMonitor}
                onStopMonitor={onStopMonitor}
            />
            {managingAssertion && (
                <AssertionActionsBuilderModal
                    urn={managingAssertion.urn}
                    assertion={managingAssertion}
                    onSubmit={onUpdateAssertion}
                    onCancel={() => setManagingAssertion(undefined)}
                />
            )}
        </>
    );
};
