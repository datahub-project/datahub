import { render } from '@testing-library/react';
import React from 'react';
import { MockedProvider } from '@apollo/client/testing';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import GlossaryRelatedTerms from '../GlossaryRelatedTerms';
import { mocks } from '../../../../../Mocks';

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
