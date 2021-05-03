import React from 'react';
import { render, waitFor } from '@testing-library/react';
import Documentation from '../Documentation';
import { sampleDocs } from '../stories/documentation';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import { MockedProvider } from '@apollo/client/testing';
import { mocks } from '../../../../../Mocks';

describe('Documentation', () => {
    it('renders', async () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks}>
                <TestPageContainer>
                    <Documentation
                        authenticatedUserUrn="urn:li:corpuser:1"
                        documents={sampleDocs}
                        updateDocumentation={(_) => undefined}
                    />
                    ,
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('Documentation')).toBeInTheDocument();
        expect(getByText('urn:li:corpuser:1')).toBeInTheDocument();
        expect(getByText('https://www.google.com')).toBeInTheDocument();
        expect(getByText('Add a link')).toBeInTheDocument();
    });
});
