import { LoadingOutlined } from '@ant-design/icons';
import { Button, Modal, Typography } from 'antd';
import React, { useEffect } from 'react';
import styled from 'styled-components';

import { AssertionStatusTag } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/preview/AssertionStatusTag';
import { RunAssertionResult } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/preview/RunAssertionResult';

import { useRunAssertionMutation } from '@graphql/assertion.generated';
import { AssertionResult, AssertionResultType } from '@types';

const LoadingIcon = styled(LoadingOutlined)`
    font-size: 22px;
    color: ${(props) => props.theme.styles['primary-color']};
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
    const [runAssertionMutation, { data, loading, error }] = useRunAssertionMutation();

    const runAssertion = () => {
        runAssertionMutation({
            variables: {
                urn,
                async: true,
                saveResult: true,
            },
        });
    };

    useEffect(() => {
        // Run assertion on first load.
        if (visible) {
            runAssertion();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [visible]);

    const result = data?.runAssertion;

    return (
        <Modal
            title="Assertion Result"
            open={visible}
            onCancel={handleClose}
            footer={
                <Button type="primary" onClick={handleClose}>
                    Ok
                </Button>
            }
        >
            {result && (
                <>
                    {[AssertionResultType.Success, AssertionResultType.Failure].includes(result.type) && (
                        <Typography.Paragraph>This assertion was successfully evaluated.</Typography.Paragraph>
                    )}
                    <Row>
                        <AssertionStatusTag assertionResultType={result.type} />
                        <RunAssertionResult result={result as AssertionResult} />
                    </Row>
                </>
            )}
            {error && (
                <Typography.Paragraph>
                    {(error?.networkError as any)?.statusCode === 503
                        ? "Oops! The assertion has exceeded the realtime results timeout (30s). Don't worry - we've still kicked off the assertion run. Check back soon to view the results!"
                        : 'Oops. An unknown error occurred while running the assertion! Try again later.'}
                </Typography.Paragraph>
            )}
            {loading && <LoadingIcon spin />}
        </Modal>
    );
};
