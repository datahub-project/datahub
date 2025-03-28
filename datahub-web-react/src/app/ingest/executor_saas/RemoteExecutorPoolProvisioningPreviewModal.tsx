import { ApolloQueryResult } from '@apollo/client';
import { Button } from '@src/alchemy-components';
import Loading from '@src/app/shared/Loading';
import {
    GetRemoteExecutorPoolQuery,
    useUpdateRemoteExecutorPoolMutation,
} from '@src/graphql/remote_executor.saas.generated';
import { RemoteExecutorPoolStatus } from '@src/types.generated';
import { message, Modal, Typography } from 'antd';
import React, { useEffect, useRef } from 'react';
import styled from 'styled-components';

const Content = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

type Props = {
    getPool: () => Promise<ApolloQueryResult<GetRemoteExecutorPoolQuery>>;
    pool: GetRemoteExecutorPoolQuery | undefined;
    visible: boolean;
    onClose: () => void;
};

export const RemoteExecutorPoolProvisioningPreviewModal = ({ getPool, pool, visible, onClose }: Props) => {
    const [updatePool] = useUpdateRemoteExecutorPoolMutation();
    const onReprovision = () => {
        if (!pool?.getRemoteExecutorPool?.urn) {
            message.error('Pool not found');
            return;
        }
        updatePool({
            variables: {
                input: { urn: pool.getRemoteExecutorPool.urn, reprovision: true },
            },
        })
            .then(() => {
                getPool();
                message.success('Pool provisioning restarted');
            })
            .catch(() => {
                message.error('Failed to retry pool provisioning');
            });
    };

    const isPending = pool?.getRemoteExecutorPool?.state?.status === RemoteExecutorPoolStatus.ProvisioningPending;
    const isNotStaged = pool?.getRemoteExecutorPool?.state?.status === undefined;
    const isPoolError = pool?.getRemoteExecutorPool?.state?.status === RemoteExecutorPoolStatus.ProvisioningFailed;

    const pollIntervalRef = useRef<NodeJS.Timeout | null>(null);
    useEffect(() => {
        if (pollIntervalRef.current) {
            clearInterval(pollIntervalRef.current);
        }
        if (!visible) {
            return () => {};
        }
        pollIntervalRef.current = setInterval(() => {
            getPool().then((result) => {
                const isSuccess = result.data?.getRemoteExecutorPool?.state?.status === RemoteExecutorPoolStatus.Ready;
                const isFailed =
                    result.data?.getRemoteExecutorPool?.state?.status === RemoteExecutorPoolStatus.ProvisioningFailed;
                const shouldStopPolling = isSuccess || isFailed;
                if (shouldStopPolling && pollIntervalRef.current !== null) {
                    clearInterval(pollIntervalRef.current);
                }
            });
        }, 1000);
        return () => {
            if (pollIntervalRef.current !== null) {
                clearInterval(pollIntervalRef.current);
            }
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [visible]);

    let content: JSX.Element | null = null;

    if (isPending) {
        content = (
            <>
                <Typography.Text>
                    Pool is staged for provisioning. Please check back soon or contact support.
                </Typography.Text>
                <Loading marginTop={0} justifyContent="flex-start" />
            </>
        );
    } else if (isPoolError) {
        content = (
            <>
                <Typography.Text>Provisioning failed. Please retry or contact support.</Typography.Text>
                <Typography.Text>
                    {pool?.getRemoteExecutorPool?.state?.message || 'Failed due to an unknown error.'}
                </Typography.Text>
                <Button style={{ alignSelf: 'flex-start' }} onClick={onReprovision}>
                    Retry
                </Button>
            </>
        );
    } else if (isNotStaged) {
        content = (
            <>
                <Typography.Text>
                    Pool provisioning has not been queued. Please retry or contact support.
                </Typography.Text>
                <Button style={{ alignSelf: 'flex-start' }} onClick={onReprovision}>
                    Retry
                </Button>
            </>
        );
    } else {
        content = (
            <>
                <Typography.Text>Provisioning pool...</Typography.Text>
                <Loading marginTop={0} justifyContent="flex-start" />
            </>
        );
    }

    return (
        <Modal open={visible} cancelButtonProps={{ style: { display: 'none' } }} onOk={onClose} title="Success">
            <div className="space-y-4 p-4">
                <Content>{content}</Content>
            </div>
        </Modal>
    );
};
