import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React from 'react';

import FieldDescription from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldDescription';
import SchemaEditableContext from '@app/shared/SchemaEditableContext';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { SchemaField, SchemaFieldDataType } from '@types';

const { mockUseEntityData } = vi.hoisted(() => ({ mockUseEntityData: vi.fn() }));

vi.mock('@app/entity/shared/EntityContext', () => ({
    useMutationUrn: () => 'urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)',
    useRefetch: () => vi.fn(),
    useEntityData: mockUseEntityData,
}));

vi.mock('@app/entityV2/shared/tabs/Dataset/Schema/SchemaContext', () => ({
    useSchemaRefetch: () => vi.fn(),
}));

vi.mock('@graphql/mutations.generated', () => ({
    useUpdateDescriptionMutation: () => [vi.fn()],
}));

const noPermissionTooltipText = 'You do not have permission to change this.';

const fieldWithDescription: SchemaField = {
    fieldPath: 'order_id',
    type: SchemaFieldDataType.Number,
    nativeDataType: 'BIGINT',
    nullable: false,
    recursive: false,
    description: 'An existing description',
} as SchemaField;

const fieldWithoutDescription: SchemaField = {
    ...fieldWithDescription,
    description: '',
};

const renderFieldDescription = (
    canEditSchemaFieldDescription: boolean,
    expandedField: SchemaField,
    isSchemaEditable = true,
) => {
    mockUseEntityData.mockReturnValue({
        entityType: 'DATASET',
        entityData: { privileges: { canEditSchemaFieldDescription } },
    });
    return render(
        <MockedProvider mocks={[]} addTypename={false}>
            <TestPageContainer>
                <SchemaEditableContext.Provider value={isSchemaEditable}>
                    <FieldDescription expandedField={expandedField} />
                </SchemaEditableContext.Provider>
            </TestPageContainer>
        </MockedProvider>,
    );
};

describe('FieldDescription edit icon permission handling', () => {
    it('enables the edit icon and shows no tooltip when the user has canEditSchemaFieldDescription', async () => {
        renderFieldDescription(true, fieldWithDescription);

        const editButton = screen.getByTestId('edit-field-description');
        expect(editButton).not.toBeDisabled();

        await userEvent.hover(editButton);
        expect(screen.queryByText(noPermissionTooltipText)).not.toBeInTheDocument();
    });

    it('disables the edit icon and shows a tooltip when the user lacks canEditSchemaFieldDescription', async () => {
        renderFieldDescription(false, fieldWithDescription);

        const editButton = screen.getByTestId('edit-field-description');
        expect(editButton).toBeDisabled();

        await userEvent.hover(editButton);
        expect(await screen.findByText(noPermissionTooltipText)).toBeInTheDocument();
    });

    it('does not render the edit icon when the schema is not editable, regardless of privilege', () => {
        renderFieldDescription(true, fieldWithDescription, false);

        expect(screen.queryByTestId('edit-field-description')).not.toBeInTheDocument();
    });
});

describe('FieldDescription add-description affordance permission handling', () => {
    it('shows no tooltip when the user has canEditSchemaFieldDescription', async () => {
        renderFieldDescription(true, fieldWithoutDescription);

        const addDescription = screen.getByText('Add Description');
        await userEvent.hover(addDescription);
        expect(screen.queryByText(noPermissionTooltipText)).not.toBeInTheDocument();
    });

    it('shows the permission tooltip when the user lacks canEditSchemaFieldDescription', async () => {
        renderFieldDescription(false, fieldWithoutDescription);

        const addDescription = screen.getByText('Add Description');
        await userEvent.hover(addDescription);
        expect(await screen.findByText(noPermissionTooltipText)).toBeInTheDocument();
    });
});
