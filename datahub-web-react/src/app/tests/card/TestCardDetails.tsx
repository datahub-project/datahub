import React, { useState } from 'react';
import styled from 'styled-components';
import { message, Modal, Tooltip, Typography } from 'antd';
import { useApolloClient } from '@apollo/client';
import { TestCardActions } from './TestCardActions';
import { Test, TestDefinitionInput } from '../../../types.generated';
import analytics, { EventType } from '../../analytics';
import { removeFromListTestsCache, updateListTestsCache } from '../cacheUtils';
import { DEFAULT_TESTS_PAGE_SIZE } from '../constants';
import { TestBuilderState } from '../builder/types';
import { useDeleteTestMutation, useUpdateTestMutation } from '../../../graphql/test.generated';
import { TestBuilderModal } from '../builder/TestBuilderModal';

const Details = styled.div`
    height: 120px;
    overflow: auto;
`;

const Header = styled.div`
    display: flex;
    align-items: top;
    justify-content: space-between;
    margin-bottom: 4px;
`;

const Title = styled(Typography.Title)`
    && {
        margin: 0px;
        padding: 0px;
    }
    :hover {
        cursor: pointer;
        text-decoration: underline;
    }
    white-space: normal;
`;

const LeftColumn = styled.div`
    max-width: 80%;
`;

const RightColumn = styled.div``;

const Description = styled(Typography.Paragraph)`
    white-space: normal;
`;

const MAX_NAME_LENGTH = 75;

type Props = {
    test: Test;
    onEdited?: (newTest) => void;
    onDeleted?: () => void;
    index: number;
};

export const TestCardDetails = ({ test, onEdited, onDeleted, index }: Props) => {
    const client = useApolloClient();
    const [updateTestMutation] = useUpdateTestMutation();
    const [deleteTestMutation] = useDeleteTestMutation();
    const [showEditTestModal, setShowEditTestModal] = useState(false);

    const editTest = (state: TestBuilderState) => {
        const newTest = {
            name: state.name as string,
            category: state.category as string,
            description: state.description as string,
            definition: state.definition as TestDefinitionInput,
        };
        updateTestMutation({
            variables: { urn: test.urn, input: newTest },
        })
            .then(() => {
                analytics.event({
                    type: EventType.UpdateTestEvent,
                });
                message.success({
                    content: `Successfully updated Test!`,
                    duration: 3,
                });
                updateListTestsCache(
                    client,
                    { __typename: 'Test', urn: test.urn, ...newTest },
                    DEFAULT_TESTS_PAGE_SIZE,
                );
                setShowEditTestModal(false);
                onEdited?.({ urn: test.urn, ...newTest });
            })
            .catch((_) => {
                message.destroy();
                message.error({
                    content: `Failed to save Test! Please review your test definition.`,
                    duration: 3,
                });
            });
    };

    const deleteTest = (urn: string) => {
        deleteTestMutation({
            variables: { urn },
        })
            .then(() => {
                analytics.event({
                    type: EventType.DeleteTestEvent,
                });
                message.success({ content: 'Removed test.', duration: 2 });
                onDeleted?.();
                removeFromListTestsCache(client, urn, DEFAULT_TESTS_PAGE_SIZE);
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to remove test! An unexpected error occurred.`, duration: 3 });
                }
            });
    };

    const confirmDelete = () => {
        Modal.confirm({
            title: `Confirm Test Removal`,
            content: `Are you sure you want to remove this test? This test will no longer be evaluated on your assets.`,
            onOk() {
                deleteTest(test.urn);
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const tooltip = test.name?.length > MAX_NAME_LENGTH ? test.name : undefined;
    const shortName = test.name?.length > MAX_NAME_LENGTH ? `${test.name.substr(0, MAX_NAME_LENGTH)}...` : test.name;

    return (
        <Details>
            <Header>
                <LeftColumn>
                    <Tooltip title={tooltip}>
                        <Title level={4} onClick={() => setShowEditTestModal(true)}>
                            {shortName}
                        </Title>
                    </Tooltip>
                </LeftColumn>
                <RightColumn>
                    <TestCardActions
                        test={test}
                        showEdit
                        showDelete
                        onClickEdit={() => setShowEditTestModal(true)}
                        onClickDelete={confirmDelete}
                        index={index}
                    />
                </RightColumn>
            </Header>
            <Description type="secondary" ellipsis={{ rows: 4, expandable: true, symbol: 'Read more' }}>
                {test.description || 'No description'}
            </Description>
            {showEditTestModal && (
                <TestBuilderModal
                    initialState={test}
                    onSubmit={editTest}
                    onCancel={() => setShowEditTestModal(false)}
                />
            )}
        </Details>
    );
};
