import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it } from 'vitest';

import CreateLogicalModelModal from '@app/entityV2/shared/logicalModels/CreateLogicalModelModal';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

describe('CreateLogicalModelModal', () => {
    it('disables create until a name and a valid column exist', () => {
        render(
            <MockedProvider mocks={[]}>
                <TestPageContainer>
                    <CreateLogicalModelModal onClose={() => {}} onCreate={() => {}} />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(screen.getByTestId('create-logical-model-button')).toBeDisabled();
    });
});
