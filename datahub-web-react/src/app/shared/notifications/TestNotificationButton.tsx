import { blue } from '@ant-design/colors';
import { SendOutlined } from '@ant-design/icons';
import { Button, message } from 'antd';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

import { TestNotificationModal } from '@app/shared/notifications/TestNotificationModal';
import {
    NotificationConnectionTestResult,
    NotificationConnectionTestStructuredReport,
    TestNotificationConfig,
} from '@app/shared/notifications/types';
import { getDestinationId } from '@app/shared/notifications/utils';
import { usePollForData } from '@app/shared/polling/utils';
import {
    useCreateNotificationConnectionTestMutation,
    useGetNotificationConnectionTestResultLazyQuery,
} from '@src/graphql/connection.generated';

const SendButton = styled(Button)`
    margin-top: 8px;
    border: 0px;
    box-shadow: none;
    padding-left: 0px;
    padding-right: 0px;
    padding-top: 0px;
    padding-bottom: 0px;
    align-self: flex-start;
    color: ${blue[4]};
`;

const POLL_INTERVAL_MILLIS = 1000;
const MAX_TIMES_TO_POLL = 10;

type Props = {
    hidden?: boolean;
} & TestNotificationConfig;

export const TestNotificationButton = ({ hidden, integration, destinationSettings, connectionUrn }: Props) => {
    // ------------- import queries ----------- //
    const [createNotificationTestRequest] = useCreateNotificationConnectionTestMutation();
    const [getNotificationTestResultRequest, { data: resultData, loading: isLoadingResultData }] =
        useGetNotificationConnectionTestResultLazyQuery();

    // Track which urn we are trying to fetch for now
    const shouldFetchForUrnRef = useRef<string>();
    // Track which urn was used when fetching the current {@var resultData}
    const resultDataIsForUrnRef = useRef<string>();

    // -------------- declare states -------------- //
    // For the modal to display results
    const [isModalVisible, setIsModalVisible] = useState(false);
    const [isLoading, setIsLoading] = useState(false);
    const destinationId = getDestinationId({ integration, destinationSettings });

    // Store parsed result in state
    const [testNotificationResult, setTestNotificationResult] = useState<NotificationConnectionTestResult>();

    // ------------- Event handlers ----------- //
    // modal management event handlers
    const onCloseModal = (stopPolling?: () => void) => {
        setTestNotificationResult(undefined);
        setIsModalVisible(false);
        setIsLoading(false);
        stopPolling?.();
    };

    // Polling management event handlers
    const onPollTimeout = useCallback(() => {
        message.error(`Timed out while waiting for results. Please try again later.`);
        setIsLoading(false);
        onCloseModal();
    }, []);
    const [startPolling, terminatePolling] = usePollForData({
        maxTimesToPoll: MAX_TIMES_TO_POLL,
        pollIntervalMillis: POLL_INTERVAL_MILLIS,
        onPollTimeout,
    });

    const pollForTestResult = (fetchForUrn: string) => {
        // Set this flag so {@link #checkHasResult} knows which urn we want to get data for
        shouldFetchForUrnRef.current = fetchForUrn;

        startPolling(() => {
            getNotificationTestResultRequest({ variables: { urn: fetchForUrn } });
            // On the next tick set this flag so {@link #checkHasResult} knows what urn the resultData is for
            setTimeout(() => {
                resultDataIsForUrnRef.current = fetchForUrn;
            }, 0);
        });
    };

    // Hook to check if the resultData is valid & handle it if so
    useEffect(() => {
        const hasResult = !!(
            !isLoadingResultData &&
            resultDataIsForUrnRef.current === shouldFetchForUrnRef.current &&
            resultData?.getNotificationConnectionTestResult?.status &&
            resultData?.getNotificationConnectionTestResult?.structuredReport?.serializedValue
        );

        if (hasResult) {
            setIsLoading(false);
            try {
                const report: NotificationConnectionTestStructuredReport = JSON.parse(
                    resultData!.getNotificationConnectionTestResult!.structuredReport!.serializedValue,
                );
                setTestNotificationResult({
                    status: resultData!.getNotificationConnectionTestResult!.status,
                    report,
                });
                terminatePolling();
            } catch (e: unknown) {
                message.error(
                    'There was an unexpected error when trying to process the test notification response. Please try again later.',
                );
                console.log('Error parsing ExecutionRequestResult', e);
            }
        }
    }, [resultData, isLoadingResultData, terminatePolling]);

    // Trigger sending the test notification and poll for results
    const testNotification = () => {
        setIsLoading(true);
        createNotificationTestRequest({ variables: { input: { urn: connectionUrn, slack: destinationSettings } } })
            .then((result) => {
                const urn = result.data?.createNotificationConnectionTest;
                if (!urn) {
                    throw new Error('Unexpected response when creation a notification test request.');
                }
                setIsModalVisible(true);
                pollForTestResult(urn);
            })
            .catch(() => {
                message.error(
                    'There was an unexpected error when trying to send a test notification. Please try again.',
                );
                setIsLoading(false);
            });
    };

    // ------------- Render ----------- //
    return hidden ? null : (
        <>
            <SendButton onClick={testNotification} disabled={isLoading}>
                <SendOutlined style={{ color: blue[4], fontSize: 12 }} />
                Send a test notification
            </SendButton>

            {isModalVisible ? (
                <TestNotificationModal
                    testNotificationResult={testNotificationResult}
                    isLoading={isLoading}
                    integrationName={integration}
                    destinationName={destinationId}
                    closeModal={() => onCloseModal(terminatePolling)}
                />
            ) : null}
        </>
    );
};
