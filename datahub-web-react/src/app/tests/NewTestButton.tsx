import { useApolloClient } from '@apollo/client';
import { message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { TestBuilderModal } from '@app/tests/builder/TestBuilderModal';
import { TestBuilderState } from '@app/tests/builder/types';
import { updateListTestsCache } from '@app/tests/cacheUtils';
import { DEFAULT_TESTS_PAGE_SIZE } from '@app/tests/constants';
import { Button } from '@src/alchemy-components';

import { useCreateTestMutation } from '@graphql/test.generated';
import { TestDefinitionInput } from '@types';

const ButtonContainer = styled.div`
    display: flex;
    justify-content: center;
`;

type Props = {
    onCreated?: (newTest) => void;
};

export const NewTestButton = ({ onCreated }: Props) => {
    const client = useApolloClient();
    const [createTestMutation] = useCreateTestMutation();
    const [showCreateTestModal, setShowCreateTestModal] = useState<boolean>(false);

    const createTest = (state: TestBuilderState) => {
        const input = {
            name: state.name as string,
            description: state.description as string,
            category: state.category as string,
            definition: state.definition as TestDefinitionInput,
        };
        createTestMutation({
            variables: { input },
        })
            .then((res) => {
                analytics.event({
                    type: EventType.CreateTestEvent,
                });
                message.success({
                    content: `Successfully created Test!`,
                    duration: 3,
                });
                const newTest = { __typename: 'Test', urn: res.data?.createTest, ...input };
                updateListTestsCache(client, newTest, DEFAULT_TESTS_PAGE_SIZE);
                onCreated?.(newTest);
                setShowCreateTestModal(false);
            })
            .catch((error) => {
                message.destroy();
                const errorMessage =
                    error.graphQLErrors?.[0]?.message || 'Failed to save Test! Please review your test definition.';
                message.error({
                    content: errorMessage,
                    duration: 5,
                });
            });
    };

    return (
        <ButtonContainer>
            <Button icon={{ icon: 'Add' }} onClick={() => setShowCreateTestModal(true)}>
                Create
            </Button>
            {showCreateTestModal && (
                <TestBuilderModal onSubmit={createTest} onCancel={() => setShowCreateTestModal(false)} />
            )}
        </ButtonContainer>
    );
};
