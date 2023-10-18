import React, { useEffect } from 'react';
import { Button, Modal, Typography } from 'antd';
import styled from 'styled-components';
import { LoadingOutlined } from '@ant-design/icons';
import { AssertionResultType, TestAssertionInput } from '../../../../../../../../../../types.generated';
import { AssertionStatusTag } from './AssertionStatusTag';
import { TestAssertionResult } from './TestAssertionResult';
import { useTestAssertionMutation } from '../../../../../../../../../../graphql/assertion.generated';

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
                            <TestAssertionResult result={data.testAssertion} />
                        </div>
                    </Row>
                </div>
            )}
            {error && (
                <Typography.Paragraph>
                    An error occurred while testing the assertion. Try again later.
                </Typography.Paragraph>
            )}
            {loading && <LoadingIcon spin />}
        </Modal>
    );
};
