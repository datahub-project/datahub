import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it } from 'vitest';

import { TestResults } from '@app/entityV2/shared/tabs/Dataset/Governance/TestResults';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { EntityType, TestResult, TestResultType } from '@types';

function makeResult(name: string, type: TestResultType): TestResult {
    return {
        type,
        test: {
            urn: `urn:li:test:${name}`,
            type: EntityType.Test,
            name,
            category: 'Governance',
            description: `${name} description`,
            definition: { json: '{}' },
        },
    };
}

describe('TestResults', () => {
    it('shows the empty state when no tests have run', () => {
        render(
            <MockedProvider mocks={[]}>
                <TestPageContainer>
                    <TestResults passing={[]} failing={[]} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText('No tests have run')).toBeInTheDocument();
        expect(screen.queryByRole('table')).not.toBeInTheDocument();
    });

    it('treats results whose test was deleted as no results', () => {
        render(
            <MockedProvider mocks={[]}>
                <TestPageContainer>
                    <TestResults passing={[{ type: TestResultType.Success, test: null }]} failing={[]} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText('No tests have run')).toBeInTheDocument();
    });

    it('lists failing results before passing ones with a summary', () => {
        render(
            <MockedProvider mocks={[]}>
                <TestPageContainer>
                    <TestResults
                        passing={[makeResult('Has owner', TestResultType.Success)]}
                        failing={[makeResult('Has domain', TestResultType.Failure)]}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText('Passing tests')).toBeInTheDocument();
        expect(screen.getByText('Failing tests')).toBeInTheDocument();
        const names = screen.getAllByText(/^Has (owner|domain)$/).map((el) => el.textContent);
        expect(names).toEqual(['Has domain', 'Has owner']);
        expect(screen.getByText('Failing')).toBeInTheDocument();
        expect(screen.getByText('Passing')).toBeInTheDocument();
    });
});
