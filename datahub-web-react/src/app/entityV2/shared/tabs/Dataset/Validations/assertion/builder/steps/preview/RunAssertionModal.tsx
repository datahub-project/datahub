import React, { useCallback, useEffect, useRef } from 'react';
import { message, Modal, Typography } from 'antd';
import styled from 'styled-components';
import { LoadingOutlined } from '@ant-design/icons';
import { usePollForData } from '@src/app/shared/polling/utils';
import { ANTD_GRAY } from '@src/app/entity/shared/constants';
import { Button } from '@src/alchemy-components';
import { ModalButtonContainer } from '@src/app/shared/button/styledComponents';
import { AssertionResult, AssertionResultType } from '../../../../../../../../../../types.generated';
import { AssertionStatusTag } from './AssertionStatusTag';
import { RunAssertionResult } from './RunAssertionResult';
import {
    useGetAssertionRunsLazyQuery,
    useRunAssertionMutation,
} from '../../../../../../../../../../graphql/assertion.generated';

const MAX_TIMES_TO_POLL = 20;
const POLL_INTERVAL_MILLIS = 3000;

const LoadingIcon = styled(LoadingOutlined)`
    font-size: 22px;
    color: ${(props) => props.theme.styles['primary-color']};
    margin-bottom: 12px;
`;

const Row = styled.div`
    display: flex;
    align-items: flex-start;
    flex-grow: 0;
    gap: 8px;
`;

type Props = {
    urn: string;
    visible: boolean;
    handleClose: () => void;
};

export const RunAssertionModal = ({ urn, visible, handleClose }: Props) => {
    const [runAssertionMutation] = useRunAssertionMutation();

    const [getAssertionRuns, { data: assertionRunsData, error, loading }] = useGetAssertionRunsLazyQuery({
        fetchPolicy: 'no-cache',
    });
    const runEvents = assertionRunsData?.assertion?.runEvents?.runEvents || [];
    const lastAssertionRun = runEvents[0];
    const lastRequestTriggeredTsRef = useRef(0);

    // Defined if we have a new run
    const assertionRun =
        lastAssertionRun && lastAssertionRun.timestampMillis > lastRequestTriggeredTsRef.current
            ? lastAssertionRun
            : undefined;
    const assertionResult = assertionRun?.result;

    // Polling management event handlers
    const onPollTimeout = useCallback(() => {
        message.error(`Timed out while waiting for results. Check the assertion page for results in a few minutes.`);
        handleClose();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);
    const [startPolling, terminatePolling] = usePollForData({
        maxTimesToPoll: MAX_TIMES_TO_POLL,
        pollIntervalMillis: POLL_INTERVAL_MILLIS,
        onPollTimeout,
    });

    const pollForAssertionResult = (requestTriggeredTime: number) => {
        // Set this flag so {@link #checkHasResult} knows when we've gotten new data
        lastRequestTriggeredTsRef.current = requestTriggeredTime;

        startPolling(() => {
            getAssertionRuns({ variables: { assertionUrn: urn, limit: 1, startTime: requestTriggeredTime + 1 } });
        });
    };

    const runAssertion = () => {
        const now = new Date().getTime();
        runAssertionMutation({
            variables: {
                urn,
                async: true,
                saveResult: true,
            },
        })
            .then(() => pollForAssertionResult(now))
            .catch((e) => {
                message.error(`Could not trigger assertion run: ${e.message}`);
                handleClose();
            });
    };

    useEffect(() => {
        // Run assertion on first load.
        if (visible) {
            runAssertion();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [visible]);

    useEffect(() => {
        if (assertionRun) {
            terminatePolling();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [assertionRun]);

    return (
        <Modal
            title="Assertion Result"
            open={visible}
            onCancel={handleClose}
            footer={
                <ModalButtonContainer>
                    <Button onClick={handleClose}>Done</Button>
                </ModalButtonContainer>
            }
        >
            {assertionResult && (
                <>
                    {[AssertionResultType.Success, AssertionResultType.Failure].includes(assertionResult.type) && (
                        <Typography.Paragraph>This assertion was successfully evaluated.</Typography.Paragraph>
                    )}
                    <Row>
                        <AssertionStatusTag assertionResultType={assertionResult.type} />
                        <RunAssertionResult result={assertionResult as AssertionResult} />
                    </Row>
                </>
            )}
            {error && (
                <Typography.Paragraph>
                    {(error?.networkError as any)?.statusCode === 503
                        ? "Oops! The assertion has exceeded the real-time results timeout (30s). Don't worry - we've still kicked off the assertion run. Check back soon to view the results!"
                        : 'Oops. An unknown error occurred while running the assertion! Try again later.'}
                </Typography.Paragraph>
            )}
            {(loading || (!error && !assertionResult)) && (
                <div>
                    <LoadingIcon spin />
                    <Typography.Paragraph style={{ fontSize: '.75rem', marginBottom: '.25rem' }}>
                        Execution has been requested.
                    </Typography.Paragraph>
                    <Typography.Paragraph style={{ color: ANTD_GRAY[7] }}>
                        It may take some time to run in your warehouse.
                        <br />
                        Feel free to close this and check the assertion page later for results.
                    </Typography.Paragraph>
                </div>
            )}
        </Modal>
    );
};
