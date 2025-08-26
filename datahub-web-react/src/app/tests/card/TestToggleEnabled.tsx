import { useApolloClient } from '@apollo/client';
import { Switch } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import handleGraphQLError from '@app/shared/handleGraphQLError';
import { updateListTestsCache } from '@app/tests/cacheUtils';
import { generateEditTestInput } from '@app/tests/card/testUtils';
import { DEFAULT_TESTS_PAGE_SIZE } from '@app/tests/constants';

import { useUpdateTestMutation } from '@graphql/test.generated';
import { Test, TestMode } from '@types';

const ToggleWrapper = styled.div`
    margin-right: 2px;
`;

interface Props {
    test: Test;
}

export default function TestToggleEnabled({ test }: Props) {
    const client = useApolloClient();
    const mode = test.status?.mode;
    const [isEnabled, setIsEnabled] = useState(mode !== undefined ? mode === TestMode.Active : true);
    const [updateTestMutation] = useUpdateTestMutation();

    const toggleEnabled = (newEnabled: boolean) => {
        setIsEnabled(newEnabled);
        const input = generateEditTestInput(test);
        const newTest = { ...input, status: { mode: newEnabled ? TestMode.Active : TestMode.Inactive } };
        updateTestMutation({
            variables: {
                urn: test.urn,
                input: newTest,
            },
        })
            .then(() => {
                analytics.event({
                    type: EventType.UpdateTestEvent,
                });
                updateListTestsCache(
                    client,
                    { __typename: 'Test', urn: test.urn, ...newTest },
                    DEFAULT_TESTS_PAGE_SIZE,
                );
            })
            .catch((error) => {
                setIsEnabled(!newEnabled);
                handleGraphQLError({
                    error,
                    defaultMessage: 'Issue updating status of metadata test. Please try again.',
                });
            });
    };

    return (
        <ToggleWrapper>
            <Switch label="" size="sm" checked={isEnabled} onChange={(e) => toggleEnabled(e.target.checked)} />
        </ToggleWrapper>
    );
}
