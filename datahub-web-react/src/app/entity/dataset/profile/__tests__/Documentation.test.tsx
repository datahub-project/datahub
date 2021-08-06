import React from 'react';
import { render } from '@testing-library/react';
import Documentation from '../Documentation';
import { sampleDocs } from '../stories/documentation';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';

describe('Documentation', () => {
    it('renders', async () => {
        const { getByText } = render(
            <TestPageContainer>
                <Documentation
                    authenticatedUserUrn="urn:li:corpuser:1"
                    authenticatedUserUsername="1"
                    documents={sampleDocs}
                    updateDocumentation={(_) => undefined}
                />
                ,
            </TestPageContainer>,
        );
        expect(getByText('Documentation')).toBeInTheDocument();
        expect(getByText('1')).toBeInTheDocument();
        expect(getByText('This doc spans the internet web')).toBeInTheDocument();
        expect(getByText('This doc spans the internet web').closest('a')).toHaveAttribute(
            'href',
            'https://www.google.com',
        );
        expect(getByText('This doc spans the internet web')).toBeInTheDocument();
        expect(getByText('Add a link')).toBeInTheDocument();
    });
});
