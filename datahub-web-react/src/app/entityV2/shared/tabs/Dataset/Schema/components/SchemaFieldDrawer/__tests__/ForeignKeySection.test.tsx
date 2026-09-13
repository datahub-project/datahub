import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it } from 'vitest';

import { sampleSchemaWithCompositeFk, sampleSchemaWithPkFk } from '@app/entityV2/dataset/profile/stories/sampleSchema';
import ForeignKeySection from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/ForeignKeySection';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { SchemaField } from '@types';

const findField = (fieldPath: string) =>
    sampleSchemaWithPkFk.fields.find((field) => field.fieldPath === fieldPath) as SchemaField;

describe('ForeignKeySection', () => {
    it('renders the related dataset and both field lists', () => {
        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <ForeignKeySection
                        expandedField={findField('shipping_address')}
                        schemaMetadata={sampleSchemaWithPkFk}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText('Foreign Key to')).toBeInTheDocument();
        expect(screen.getByText('Yet Another Dataset')).toBeInTheDocument();
        expect(screen.getByText('Source fields')).toBeInTheDocument();
        expect(screen.getByText('Target fields')).toBeInTheDocument();
        expect(screen.getByTestId('foreign-key-constraint')).toBeInTheDocument();
    });

    it('renders nothing for a field without foreign keys', () => {
        const { container } = render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <ForeignKeySection expandedField={findField('id')} schemaMetadata={sampleSchemaWithPkFk} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(container).toBeEmptyDOMElement();
    });
    it('renders every field of a composite constraint and a placeholder for an unresolved dataset', () => {
        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <ForeignKeySection
                        expandedField={findField('shipping_address')}
                        schemaMetadata={sampleSchemaWithCompositeFk}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByTestId('foreign-key-composite_constraint')).toBeInTheDocument();
        expect(screen.getByText('address_id')).toBeInTheDocument();
        expect(screen.getByTestId('foreign-key-unresolved_constraint')).toBeInTheDocument();
        expect(screen.getByText('Related dataset is unavailable')).toBeInTheDocument();
    });
});
