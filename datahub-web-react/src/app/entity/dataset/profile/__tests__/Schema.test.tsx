import React from 'react';
import { render } from '@testing-library/react';
import Schema from '../schema/Schema';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import { sampleSchema } from '../stories/sampleSchema';

describe('Schema', () => {
    it('renders', () => {
        const { getByText, queryAllByTestId } = render(
            <TestPageContainer>
                <Schema schema={sampleSchema} />
            </TestPageContainer>,
        );
        expect(getByText('name')).toBeInTheDocument();
        expect(getByText('the name of the order')).toBeInTheDocument();
        expect(getByText('shipping_address')).toBeInTheDocument();
        expect(getByText('the address the order ships to')).toBeInTheDocument();
        expect(queryAllByTestId('icon-STRING')).toHaveLength(2);
    });
});
