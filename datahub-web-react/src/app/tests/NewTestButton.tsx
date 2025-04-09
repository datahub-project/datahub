import React, { useState } from 'react';
import styled from 'styled-components';
import { useApolloClient } from '@apollo/client';
import { Button } from '@src/alchemy-components';
import { message } from 'antd';
import { useCreateTestMutation } from '../../graphql/test.generated';
import { TestBuilderModal } from './builder/TestBuilderModal';
import { DEFAULT_TESTS_PAGE_SIZE } from './constants';
import { TestBuilderState } from './builder/types';
import analytics, { EventType } from '../analytics';
import { updateListTestsCache } from './cacheUtils';
import { TestDefinitionInput } from '../../types.generated';

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
            .catch((_) => {
                message.destroy();
                message.error({
                    content: `Failed to save Test! Please review your test definition.`,
                    duration: 3,
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
