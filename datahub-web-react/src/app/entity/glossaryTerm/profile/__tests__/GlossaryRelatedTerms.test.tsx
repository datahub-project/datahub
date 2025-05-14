import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';

import GlossaryRelatedTerms from '@app/entity/glossaryTerm/profile/GlossaryRelatedTerms';
import { mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

describe('Glossary Related Terms', () => {
    it('renders and print hasRelatedTerms detail by default', async () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <GlossaryRelatedTerms />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('Contains')).toBeInTheDocument();
        expect(getByText('Inherits')).toBeInTheDocument();
    });
});
