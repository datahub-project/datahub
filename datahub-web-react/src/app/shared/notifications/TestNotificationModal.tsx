import { green, red } from '@ant-design/colors';
import { CheckOutlined, CloseOutlined } from '@ant-design/icons';
import { Modal, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ModalButtonContainer } from '@app/shared/button/styledComponents';
import { TEST_NOTIFICATION_RESULT_STATUS } from '@app/shared/notifications/constants';
import { Button } from '@src/alchemy-components';
import { ANTD_GRAY } from '@src/app/entity/shared/constants';
import Loading from '@src/app/shared/Loading';
import { NotificationConnectionTestResult } from '@src/app/shared/notifications/types';
import { getErrorDisplayContentFromSlackErrorCode } from '@src/app/shared/notifications/utils';
import { capitalizeFirstLetter } from '@src/app/shared/textUtil';

const ModalHeader = styled.div`
    align-items: center;
    display: flex;
    padding: 10px 10px 0 10px;
    padding: 5px;
    font-size: 18px;
`;
const ResultsWrapper = styled.div`
    padding: 0 10px;
`;
const LoadingHeader = styled(Typography.Title)`
    display: flex;
`;
const LoadingSubheader = styled.div`
    display: flex;
    font-size: 12px;
    color: ${ANTD_GRAY[7]};
`;
const LoadingWrapper = styled.div`
    display: flex;
    margin-top: 16px;
`;
const ResultsHeader = styled.div<{ success: boolean }>`
    align-items: center;
    color: ${(props) => (props.success ? `${green[6]}` : `${red[5]}`)};
    display: flex;
    margin-bottom: 5px;
    font-size: 20px;
    font-weight: 550;

    svg {
        margin-right: 6px;
    }
`;
const StyledClose = styled(CloseOutlined)`
    color: ${red[5]};
    margin-right: 2px;
`;

const StyledCheck = styled(CheckOutlined)`
    color: ${green[6]};
    margin-right: 2px;
`;

const ResultsBody = styled.div`
    display: flex;
    margin-top: 12px;
`;
const ResultsBodyText = styled(Typography.Paragraph)`
    margin-bottom: 0px !important;
    font-size: 14px;
    color: ${ANTD_GRAY[8]};
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
        !isLoading && testNotificationResult?.status.toUpperCase() === TEST_NOTIFICATION_RESULT_STATUS.ERROR;

    // There will only be 1 report since we're only sending to 1 channel/user at a time.
    // TODO: handle multiple channels being tested at once
    const firstReportWithError = testNotificationResult?.report.find((report) => report.error);
    const errorMessage =
        firstReportWithError?.error &&
        getErrorDisplayContentFromSlackErrorCode(firstReportWithError.error, {
            destinationName,
        });

    return (
        <Modal
            open
            onCancel={closeModal}
            footer={
                <ModalButtonContainer>
                    <Button onClick={closeModal}>Done</Button>
                </ModalButtonContainer>
            }
            title={
                <ModalHeader style={{ margin: 0 }}>
                    {capitalizeFirstLetter(integrationName)} Notification Test
                </ModalHeader>
            }
            width={750}
        >
            {isLoading ? (
                <ResultsWrapper>
                    <LoadingHeader level={4}>Sending a test notification to {destinationName}...</LoadingHeader>
                    <LoadingSubheader>This could take a few seconds.</LoadingSubheader>
                    <LoadingWrapper>
                        <Loading height={36} marginTop={0} justifyContent="flex-start" />
                    </LoadingWrapper>
                </ResultsWrapper>
            ) : (
                <ResultsWrapper>
                    <ResultsHeader success={!isSendingFailed}>
                        {isSendingFailed ? (
                            <>
                                <StyledClose /> Failed to send test notification
                            </>
                        ) : (
                            <>
                                <StyledCheck /> Successfully sent notification.
                            </>
                        )}
                    </ResultsHeader>
                    <ResultsBody>
                        <ResultsBodyText>
                            {isSendingFailed ? errorMessage : `Visit ${integrationName} to confirm you've received it.`}
                        </ResultsBodyText>
                    </ResultsBody>
                </ResultsWrapper>
            )}
        </Modal>
    );
};
