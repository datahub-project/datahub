import { LoadingOutlined } from '@ant-design/icons';
import { Button, Modal, Typography } from 'antd';
import React, { useEffect } from 'react';
import styled from 'styled-components';

import { AssertionStatusTag } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/preview/AssertionStatusTag';
import { RunAssertionResult } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/preview/RunAssertionResult';

import { useTestAssertionMutation } from '@graphql/assertion.generated';
import { AssertionResultType, TestAssertionInput } from '@types';

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
    visible: boolean;
    handleClose: () => void;
    input: TestAssertionInput;
};

export const TestAssertionModal = ({ visible, handleClose, input }: Props) => {
    const [testAssertionMutation, { data, loading, error }] = useTestAssertionMutation();

    const handleTestAssertion = () => {
        testAssertionMutation({
            variables: {
                input,
            },
        });
    };

    useEffect(() => {
        if (visible) {
            handleTestAssertion();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [visible]);

    const getErrorMessage = (errorData: any): string => {
        if ((errorData?.networkError as any)?.statusCode === 503) {
            return "Oops! The assertion has exceeded the real-time results timeout (30s). Don't worry - we've still kicked off the assertion run. Check back soon to view the results!";
        }

        if (errorData?.graphQLErrors?.[0]?.extensions?.code === 400) {
            return `This assertion can not be tested due to: ${errorData.message}`;
        }

        return 'Oops. An unknown error occurred while testing the assertion! Try again later.';
    };

    return (
        <Modal
            title="Assertion Result"
            open={visible}
            onCancel={handleClose}
            footer={
                <Button type="primary" onClick={handleClose}>
                    OK
                </Button>
            }
        >
            {data?.testAssertion && (
                <div>
                    {[AssertionResultType.Success, AssertionResultType.Failure].includes(data.testAssertion.type) && (
                        <Typography.Paragraph>This query ran successfully.</Typography.Paragraph>
                    )}
                    <Row>
                        <AssertionStatusTag assertionResultType={data.testAssertion.type} />
                        <div>
                            <RunAssertionResult result={data.testAssertion} isTest />
                        </div>
                    </Row>
                </div>
            )}
            {error && <Typography.Paragraph>{getErrorMessage(error)}</Typography.Paragraph>}
            {loading && <LoadingIcon spin />}
        </Modal>
    );
};
