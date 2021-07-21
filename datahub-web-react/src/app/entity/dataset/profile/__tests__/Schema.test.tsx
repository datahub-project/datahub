import React from 'react';
import { fireEvent, render } from '@testing-library/react';
import Schema from '../schema/Schema';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import { sampleSchema, sampleSchemaWithTags } from '../stories/sampleSchema';

describe('Schema', () => {
    it('renders', () => {
        const { getByText, queryAllByTestId } = render(
            <TestPageContainer>
                <Schema urn={sampleSchema?.platformUrn || ''} schema={sampleSchema} updateEditableSchema={jest.fn()} />
            </TestPageContainer>,
        );
        expect(getByText('name')).toBeInTheDocument();
        expect(getByText('the name of the order')).toBeInTheDocument();
        expect(getByText('shipping_address')).toBeInTheDocument();
        expect(getByText('the address the order ships to')).toBeInTheDocument();
        expect(queryAllByTestId('icon-STRING')).toHaveLength(2);
    });

    it('renders raw', () => {
        const { getByText, queryAllByTestId } = render(
            <TestPageContainer>
                <Schema urn={sampleSchema?.platformUrn || ''} schema={sampleSchema} updateEditableSchema={jest.fn()} />
            </TestPageContainer>,
        );

        expect(queryAllByTestId('icon-STRING')).toHaveLength(2);
        expect(queryAllByTestId('schema-raw-view')).toHaveLength(0);

        const rawButton = getByText('Raw');
        fireEvent.click(rawButton);

        expect(queryAllByTestId('icon-STRING')).toHaveLength(0);
        expect(queryAllByTestId('schema-raw-view')).toHaveLength(1);

        const schemaButton = getByText('Tabular');
        fireEvent.click(schemaButton);

        expect(queryAllByTestId('icon-STRING')).toHaveLength(2);
        expect(queryAllByTestId('schema-raw-view')).toHaveLength(0);
    });

    it('renders tags and terms', () => {
        const { getByText } = render(
            <TestPageContainer>
                <Schema
                    urn={sampleSchema?.platformUrn || ''}
                    schema={sampleSchemaWithTags}
                    updateEditableSchema={jest.fn()}
                />
            </TestPageContainer>,
        );
        expect(getByText('Legacy')).toBeInTheDocument();
        expect(getByText('sample-glossary-term')).toBeInTheDocument();
    });

    it('renders description', () => {
        const { getByText } = render(
            <TestPageContainer>
                <Schema
                    urn={sampleSchema?.platformUrn || ''}
                    schema={sampleSchemaWithTags}
                    updateEditableSchema={jest.fn()}
                />
            </TestPageContainer>,
        );
        expect(getByText('order id')).toBeInTheDocument();
    });

    it('renders field', () => {
        const { getByText } = render(
            <TestPageContainer>
                <Schema
                    urn={sampleSchema?.platformUrn || ''}
                    schema={sampleSchemaWithTags}
                    updateEditableSchema={jest.fn()}
                />
            </TestPageContainer>,
        );
        expect(getByText('shipping_address')).toBeInTheDocument();
    });
});
