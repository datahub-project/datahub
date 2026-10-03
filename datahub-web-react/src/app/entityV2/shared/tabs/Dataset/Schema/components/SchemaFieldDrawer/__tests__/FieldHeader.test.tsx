import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { sampleSchemaWithPkFk } from '@app/entityV2/dataset/profile/stories/sampleSchema';
import FieldHeader from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldHeader';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { SchemaField } from '@types';

const findField = (fieldPath: string) =>
    sampleSchemaWithPkFk.fields.find((field) => field.fieldPath === fieldPath) as SchemaField;

const renderHeader = (fieldPath: string) =>
    render(
        <MockedProvider mocks={[]} addTypename={false}>
            <TestPageContainer>
                <FieldHeader
                    expandedField={findField(fieldPath)}
                    setExpandedDrawerFieldPath={vi.fn()}
                    schemaMetadata={sampleSchemaWithPkFk}
                />
            </TestPageContainer>
        </MockedProvider>,
    );

describe('FieldHeader', () => {
    it('marks a field that participates in a foreign key', () => {
        renderHeader('shipping_address');

        expect(screen.getByText('Foreign Key')).toBeInTheDocument();
        expect(screen.queryByRole('button', { name: 'Foreign Key' })).not.toBeInTheDocument();
    });

    it('does not mark a field without foreign keys', () => {
        renderHeader('count');

        expect(screen.queryByText('Foreign Key')).not.toBeInTheDocument();
    });
});
