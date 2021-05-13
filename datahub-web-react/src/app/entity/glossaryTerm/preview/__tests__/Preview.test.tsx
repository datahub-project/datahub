import { render } from '@testing-library/react';
import React from 'react';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import { Preview } from '../Preview';

describe('Preview', () => {
    it('renders', () => {
        const { getByText } = render(
            <TestPageContainer>
                <Preview
                    urn="urn:li:glossaryTerm:instruments.FinancialInstrument_v1"
                    name="name"
                    definition="definition"
                    owners={null}
                />
            </TestPageContainer>,
        );
        expect(getByText('definition')).toBeInTheDocument();
    });
});
