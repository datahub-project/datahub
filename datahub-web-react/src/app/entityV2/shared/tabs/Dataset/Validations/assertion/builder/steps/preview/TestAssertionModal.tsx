import { LoadingOutlined } from '@ant-design/icons';
import { ApolloError } from '@apollo/client';
import { Modal, Typography } from 'antd';
import React, { useEffect } from 'react';
import styled from 'styled-components';

import { AssertionStatusTag } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/preview/AssertionStatusTag';
import { RunAssertionResult } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/preview/RunAssertionResult';
import { Button } from '@src/alchemy-components';

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

const FooterButtons = styled.div`
    display: flex;
    justify-content: flex-end;
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

    const getErrorMessage = (errorData: ApolloError): string => {
        if (
            errorData.networkError &&
            'statusCode' in errorData.networkError &&
            errorData.networkError.statusCode === 503
        ) {
            return 'Oops! The assertion has exceeded the real-time results timeout (30s). Create the assertion to run it to completion!';
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
                <FooterButtons>
                    <Button variant="outline" onClick={handleClose}>
                        Done
                    </Button>
                </FooterButtons>
            }
        >
            {data?.testAssertion && (
                <>
                    {[AssertionResultType.Success, AssertionResultType.Failure].includes(data.testAssertion.type) && (
                        <Typography.Paragraph>The assertion was evaluated successfully.</Typography.Paragraph>
                    )}
                    <Row>
                        <AssertionStatusTag assertionResultType={data.testAssertion.type} />
                        <RunAssertionResult result={data.testAssertion} isTest expectedFromTestInput={input} />
                    </Row>
                </>
            )}
            {error && <Typography.Paragraph>{getErrorMessage(error)}</Typography.Paragraph>}
            {loading && <LoadingIcon spin />}
        </Modal>
    );
};
