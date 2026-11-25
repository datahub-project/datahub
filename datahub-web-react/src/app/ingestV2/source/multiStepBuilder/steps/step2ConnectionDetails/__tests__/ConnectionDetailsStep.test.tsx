import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { ConnectionDetailsStep } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/ConnectionDetailsStep';
import { MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';
import { MultiStepFormProvider } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

// The sections pull in GraphQL and large form trees that the banner gating does not use.
vi.mock('@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/NameAndOwnersSection', () => ({
    NameAndOwnersSection: () => null,
}));
vi.mock(
    '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/RecipeSection',
    () => ({
        RecipeSection: () => null,
    }),
);
vi.mock(
    '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/sections/syncScheduleSection/ScheduleSection',
    () => ({ ScheduleSection: () => null }),
);
vi.mock('@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/AdvancedSection', () => ({
    AdvancedSection: () => null,
}));

const BANNER_TEXT = /Snowflake is deprecating username \+ password authentication/;

function renderStep(type: string, config: Record<string, string>) {
    const state: MultiStepSourceBuilderState = {
        type,
        config: { recipe: JSON.stringify({ source: { type, config } }) },
    };
    return render(
        <MockedProvider mocks={[]} addTypename={false}>
            <TestPageContainer>
                <MultiStepFormProvider steps={[]} initialState={state}>
                    <ConnectionDetailsStep />
                </MultiStepFormProvider>
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('ConnectionDetailsStep Snowflake password-auth deprecation banner', () => {
    it('shows the banner for a Snowflake password-auth recipe', () => {
        const { getByText, container } = renderStep('snowflake', {
            authentication_type: 'DEFAULT_AUTHENTICATOR',
            password: 'secret', // noqa: secret gitleaks:allow - dummy test fixture
        });

        expect(getByText(BANNER_TEXT)).toBeInTheDocument();
        expect(container.querySelector('a[href$="migrate-to-key-pair-auth"]')).not.toBeNull();
    });

    it('hides the banner for a Snowflake key-pair recipe', () => {
        const { queryByText } = renderStep('snowflake', {
            authentication_type: 'KEY_PAIR_AUTHENTICATOR',
            private_key: '-----BEGIN PRIVATE KEY-----',
        });

        expect(queryByText(BANNER_TEXT)).toBeNull();
    });

    it('hides the banner for a non-Snowflake recipe with a password', () => {
        const { queryByText } = renderStep('mysql', {
            password: 'secret', // noqa: secret gitleaks:allow - dummy test fixture
        });

        expect(queryByText(BANNER_TEXT)).toBeNull();
    });
});
