import React from 'react';
import styled from 'styled-components';

import { TEST_NOTIFICATION_RESULT_STATUS } from '@app/shared/notifications/constants';
import { Heading, Icon, Loader, Modal, Text } from '@src/alchemy-components';
import { NotificationConnectionTestResult } from '@src/app/shared/notifications/types';
import { getErrorDisplayContentFromErrorCode } from '@src/app/shared/notifications/utils';
import { capitalizeFirstLetter } from '@src/app/shared/textUtil';

const ResultsWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const LoadingContent = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    align-items: center;
    text-align: center;
`;

const LoadingContainer = styled.div`
    display: flex;
    justify-content: center;
    margin-top: 16px;
`;

const ResultsContent = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
`;

const ResultsHeader = styled.div<{ success: boolean }>`
    display: flex;
    align-items: center;
    gap: 8px;
    color: ${(props) => (props.success ? '#52C41A' : '#FF4D4F')};
`;

type Props = {
    testNotificationResult?: NotificationConnectionTestResult;
    isLoading: boolean;
    integrationName: string;
    destinationName: string;
    closeModal: () => void;
};

export const TestNotificationModal = ({
    testNotificationResult,
    isLoading,
    integrationName,
    destinationName,
    closeModal,
}: Props) => {
    const isSendingFailed =
        !isLoading && testNotificationResult?.status?.toUpperCase() === TEST_NOTIFICATION_RESULT_STATUS.ERROR;

    // There will only be 1 report since we're only sending to 1 channel/user at a time.
    // TODO: handle multiple channels being tested at once
    const firstReportWithError = testNotificationResult?.report?.find((report) => report.error);
    let errorMessage = 'An unknown error occurred while sending the notification. Please try again.';
    if (firstReportWithError?.error) {
        errorMessage = getErrorDisplayContentFromErrorCode(firstReportWithError.error, {
            destinationName,
        });
    } else if (testNotificationResult?.report?.[0]?.errorType) {
        errorMessage = getErrorDisplayContentFromErrorCode(testNotificationResult.report[0].errorType, {
            destinationName,
        });
    }

    const doneButton = {
        text: 'Done',
        variant: 'outline' as const,
        onClick: closeModal,
        buttonDataTestId: 'test-notification-modal-done-button',
    };

    return (
        <Modal
            title={`${capitalizeFirstLetter(integrationName)} Notification Test`}
            onCancel={closeModal}
            buttons={[doneButton]}
            dataTestId="test-notification-modal"
            width={750}
        >
            <ResultsWrapper>
                {isLoading ? (
                    <LoadingContent>
                        <Heading type="h3" color="gray" colorLevel={600} weight="medium">
                            Sending a test notification to {destinationName}...
                        </Heading>
                        <Text type="span" color="gray" colorLevel={1700} size="sm">
                            This could take a few seconds.
                        </Text>
                        <LoadingContainer>
                            <Loader size="md" />
                        </LoadingContainer>
                    </LoadingContent>
                ) : (
                    <ResultsContent>
                        <ResultsHeader success={!isSendingFailed}>
                            <Icon icon={isSendingFailed ? 'XCircle' : 'CheckCircle'} source="phosphor" size="lg" />
                            <Heading type="h3" color={isSendingFailed ? 'red' : 'green'} weight="medium">
                                {isSendingFailed
                                    ? 'Failed to send test notification'
                                    : 'Successfully sent notification'}
                            </Heading>
                        </ResultsHeader>
                        <Text type="span" color="gray" colorLevel={1700}>
                            {isSendingFailed ? errorMessage : `Visit ${integrationName} to confirm you've received it.`}
                        </Text>
                    </ResultsContent>
                )}
            </ResultsWrapper>
        </Modal>
    );
};
