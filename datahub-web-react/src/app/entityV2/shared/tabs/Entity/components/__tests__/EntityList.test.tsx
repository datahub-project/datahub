import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it } from 'vitest';

import { EntityList } from '@app/entityV2/shared/tabs/Entity/components/EntityList';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { EntityType } from '@types';

describe('EntityList', () => {
    it('skips related entities that did not resolve', () => {
        render(
            <MockedProvider mocks={[]}>
                <TestPageContainer>
                    <EntityList type={EntityType.Dataset} entities={[null, undefined]} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText(/^0 /)).toBeInTheDocument();
    });
});
