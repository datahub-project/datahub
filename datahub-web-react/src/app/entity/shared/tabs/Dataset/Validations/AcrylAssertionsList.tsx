import React, { useState } from 'react';
import { message, Modal } from 'antd';
import { Assertion, Monitor, MonitorMode, DataContract } from '../../../../../../types.generated';
import { useDeleteAssertionMutation } from '../../../../../../graphql/assertion.generated';
import { useUpdateMonitorStatusMutation, useDeleteMonitorMutation } from '../../../../../../graphql/monitor.generated';
import { AssertionActionsBuilderModal } from './assertion/builder/AssertionActionsBuilderModal';
import { AcrylAssertionsTable } from './AcrylAssertionsTable';
import analytics, { EventType } from '../../../../../analytics';
import { AssertionViewDetailsModal } from './assertion/builder/AssertionViewDetailsModal';
import { useUpsertDataContractMutation } from '../../../../../../graphql/contract.generated';
import {
    buildAddAssertionToContractMutationVariables,
    buildRemoveAssertionFromContractMutationVariables,
} from './contract/builder/utils';
import { DataContractCategoryType } from './contract/builder/types';

type Props = {
    assertions: Array<Assertion>;
    contract?: DataContract;
    showMenu?: boolean;
    showDetails?: boolean;
    showSelect?: boolean;
    selectedUrns?: string[];
    onDeletedAssertion?: (urn: string) => void;
    onUpdatedAssertion?: (assertion: Assertion) => void;
    onSelect?: (assertionUrn: string) => void;
};

/**
 * Acryl-specific list of assertions displaying their most recent run status, their human-readable
 * description, and platform.
 *
 * Currently this component supports rendering Dataset Assertions only.
 */
export const AcrylDatasetAssertionsList = ({
    assertions,
    contract,
    showMenu,
    showDetails,
    showSelect,
    selectedUrns,
    onDeletedAssertion,
    onUpdatedAssertion,
    onSelect,
}: Props) => {
    const [deleteAssertionMutation] = useDeleteAssertionMutation();
    const [deleteMonitorMutation] = useDeleteMonitorMutation();
    const [upsertDataContractMutation] = useUpsertDataContractMutation();
    const [updateMonitorStatusMutation] = useUpdateMonitorStatusMutation();
    const [managingAssertion, setManagingAssertion] = useState<Assertion | undefined>(undefined);
    const [viewingAssertionDetails, setViewingAssertionDetails] = useState<Assertion | undefined>(undefined);

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

    const onViewAssertionDetails = (urn: string) => {
        setViewingAssertionDetails(assertions.find((assertion) => assertion.urn === urn));
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
                            ...(updatedAssertion as any)?.monitor,
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
                            ...(updatedAssertion as any)?.monitor,
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

    const onAddToContract = (category: DataContractCategoryType, entityUrn: string, assertionUrn: string) => {
        upsertDataContractMutation({
            variables: buildAddAssertionToContractMutationVariables(category, entityUrn, assertionUrn, contract),
        })
            .then(({ data, errors }) => {
                if (!errors) {
                    message.success({ content: 'Added assertion to contract!', duration: 2 });
                    const updatedAssertion = assertions.find((assertion) => assertion.urn === assertionUrn);
                    onUpdateAssertion?.({
                        ...updatedAssertion,
                        contract: {
                            start: 0,
                            count: 1,
                            total: 1,
                            relationships: [
                                {
                                    entity: data?.upsertDataContract as any,
                                },
                            ],
                        },
                    } as Assertion);
                }
            })
            .catch(() => {
                message.destroy();
                message.error({ content: 'Failed to add Assertion to Contract. An unexpected error occurred' });
            });
    };

    const onRemoveFromContract = (entityUrn: string, assertionUrn: string) => {
        upsertDataContractMutation({
            variables: buildRemoveAssertionFromContractMutationVariables(entityUrn, assertionUrn, contract),
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({ content: 'Removed assertion from contract.', duration: 2 });
                    const updatedAssertion = assertions.find((assertion) => assertion.urn === assertionUrn);
                    onUpdateAssertion?.({
                        ...updatedAssertion,
                        contract: {
                            start: 0,
                            count: 0,
                            total: 0,
                            relationships: [],
                        },
                    } as Assertion);
                }
            })
            .catch(() => {
                message.destroy();
                message.error({ content: 'Failed to remove Assertion from Contract. An unexpected error occurred' });
            });
    };

    return (
        <>
            <AcrylAssertionsTable
                assertions={assertions}
                contract={contract}
                onManageAssertion={onManageAssertion}
                onDeleteAssertion={onDeleteAssertion}
                onViewAssertionDetails={onViewAssertionDetails}
                onStartMonitor={onConfirmStartMonitor}
                onStopMonitor={onStopMonitor}
                onAddToContract={onAddToContract}
                onRemoveFromContract={onRemoveFromContract}
                showMenu={showMenu}
                showDetails={showDetails}
                showSelect={showSelect}
                selectedUrns={selectedUrns}
                onSelect={onSelect}
            />
            {managingAssertion && (
                <AssertionActionsBuilderModal
                    urn={managingAssertion.urn}
                    assertion={managingAssertion}
                    onSubmit={onUpdateAssertion}
                    onCancel={() => setManagingAssertion(undefined)}
                />
            )}
            {viewingAssertionDetails && (
                <AssertionViewDetailsModal
                    assertion={viewingAssertionDetails}
                    onCancel={() => setViewingAssertionDetails(undefined)}
                />
            )}
        </>
    );
};
