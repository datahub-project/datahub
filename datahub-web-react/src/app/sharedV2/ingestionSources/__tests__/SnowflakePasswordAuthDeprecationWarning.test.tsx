import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';

import { SnowflakePasswordAuthDeprecationWarning } from '@app/sharedV2/ingestionSources/SnowflakePasswordAuthDeprecationWarning';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

describe('SnowflakePasswordAuthDeprecationWarning', () => {
    it('renders the deprecation banner when the recipe uses DEFAULT_AUTHENTICATOR', () => {
        const recipe = {
            source: { config: { authentication_type: 'DEFAULT_AUTHENTICATOR', password: 'secret' } }, // noqa: secret gitleaks:allow - dummy test fixture
        };

        const { getByText, getByRole } = render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <SnowflakePasswordAuthDeprecationWarning recipe={recipe} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(getByText(/Snowflake is deprecating username \+ password authentication/)).toBeInTheDocument();
        // The migration guide link is present with its text and href.
        const link = getByRole('link');
        expect(link.getAttribute('href')).toBe(
            'https://docs.datahub.com/docs/quick-ingestion-guides/snowflake/migrate-to-key-pair-auth',
        );
        expect(link).toHaveTextContent('migration guide');
    });

    it('renders nothing when the recipe uses key-pair auth', () => {
        const recipe = {
            source: {
                config: {
                    authentication_type: 'KEY_PAIR_AUTHENTICATOR',
                    private_key: '-----BEGIN PRIVATE KEY-----',
                },
            },
        };

        const { container } = render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <SnowflakePasswordAuthDeprecationWarning recipe={recipe} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(container).toBeEmptyDOMElement();
    });

    it('renders nothing when the recipe is missing', () => {
        const { container } = render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <SnowflakePasswordAuthDeprecationWarning recipe={null} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(container).toBeEmptyDOMElement();
    });

    it('renders nothing when key-pair auth has a stale password (respects explicit authentication_type)', () => {
        const recipe = {
            source: {
                config: {
                    authentication_type: 'KEY_PAIR_AUTHENTICATOR',
                    password: 'secret', // stale password left over from a previous config // noqa: secret gitleaks:allow - dummy test fixture
                    private_key: '-----BEGIN PRIVATE KEY-----',
                },
            },
        };

        const { container } = render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <SnowflakePasswordAuthDeprecationWarning recipe={recipe} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(container).toBeEmptyDOMElement();
    });

    it('infers DEFAULT_AUTHENTICATOR from a password with no explicit authentication_type', () => {
        const recipe = { source: { config: { password: 'secret' } } }; // noqa: secret gitleaks:allow - dummy test fixture

        const { getByText } = render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <SnowflakePasswordAuthDeprecationWarning recipe={recipe} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(getByText(/Snowflake is deprecating username \+ password authentication/)).toBeInTheDocument();
    });

    it('renders nothing for an OAuth recipe (explicit auth type is not DEFAULT_AUTHENTICATOR)', () => {
        // getSnowflakeAuthTypeFromRecipe returns the explicit auth type verbatim, so an
        // OAuth recipe must not be treated as password auth. This is also the value the
        // auth-type select receives; its options are only KEY_PAIR / DEFAULT, so an OAuth
        // recipe shows an out-of-options value in the form — documented in the PR body.
        const recipe = { source: { config: { authentication_type: 'OAUTH_AUTHENTICATOR' } } };

        const { container } = render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <SnowflakePasswordAuthDeprecationWarning recipe={recipe} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(container).toBeEmptyDOMElement();
    });
});
