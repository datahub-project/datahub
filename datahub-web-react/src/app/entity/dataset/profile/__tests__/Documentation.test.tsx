import React from 'react';
import { render } from '@testing-library/react';
import Documentation from '../Documentation';
import { sampleDocs } from '../stories/documentation';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';

describe('Documentation', () => {
    it('renders', () => {
        const { getByText } = render(
            <TestPageContainer>
                <Documentation documents={sampleDocs} updateDocumentation={(_) => undefined} />,
            </TestPageContainer>,
        );
        expect(getByText('Documentation')).toBeInTheDocument();
        expect(getByText('urn:li:corpuser:1')).toBeInTheDocument();
        expect(getByText('https://www.google.com')).toBeInTheDocument();
        expect(getByText('Add a link')).toBeInTheDocument();
    });
});
