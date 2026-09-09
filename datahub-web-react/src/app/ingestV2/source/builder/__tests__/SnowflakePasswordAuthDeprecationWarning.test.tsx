import { render } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';

import { SnowflakePasswordAuthDeprecationWarning } from '@app/ingestV2/source/builder/SnowflakePasswordAuthDeprecationWarning';
import themeV2 from '@conf/theme/themeV2';

describe('SnowflakePasswordAuthDeprecationWarning', () => {
    it('renders the deprecation banner when the recipe uses DEFAULT_AUTHENTICATOR', () => {
        const recipe = {
            source: { config: { authentication_type: 'DEFAULT_AUTHENTICATOR', password: 'secret' } }, // noqa: secret gitleaks:allow - dummy test fixture
        };

        const { getByText, getByRole } = render(
            <ThemeProvider theme={themeV2}>
                <SnowflakePasswordAuthDeprecationWarning recipe={recipe} />
            </ThemeProvider>,
        );

        expect(getByText(/Snowflake is deprecating username \+ password authentication/)).toBeInTheDocument();
        // The migration guide link is present.
        const link = getByRole('link');
        expect(link.getAttribute('href')).toBe(
            'https://docs.datahub.com/docs/quick-ingestion-guides/snowflake/migrate-to-key-pair-auth',
        );
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
            <ThemeProvider theme={themeV2}>
                <SnowflakePasswordAuthDeprecationWarning recipe={recipe} />
            </ThemeProvider>,
        );

        expect(container).toBeEmptyDOMElement();
    });

    it('renders nothing when the recipe is missing', () => {
        const { container } = render(
            <ThemeProvider theme={themeV2}>
                <SnowflakePasswordAuthDeprecationWarning recipe={null} />
            </ThemeProvider>,
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
            <ThemeProvider theme={themeV2}>
                <SnowflakePasswordAuthDeprecationWarning recipe={recipe} />
            </ThemeProvider>,
        );

        expect(container).toBeEmptyDOMElement();
    });

    it('infers DEFAULT_AUTHENTICATOR from a password with no explicit authentication_type', () => {
        const recipe = { source: { config: { password: 'secret' } } }; // noqa: secret gitleaks:allow - dummy test fixture

        const { getByText } = render(
            <ThemeProvider theme={themeV2}>
                <SnowflakePasswordAuthDeprecationWarning recipe={recipe} />
            </ThemeProvider>,
        );

        expect(getByText(/Snowflake is deprecating username \+ password authentication/)).toBeInTheDocument();
    });
});
