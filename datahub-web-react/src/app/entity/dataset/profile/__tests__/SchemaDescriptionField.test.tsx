import React from 'react';
import { fireEvent, render, waitFor } from '@testing-library/react';
import SchemaDescriptionField from '../schema/components/SchemaDescriptionField';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';

describe('SchemaDescriptionField', () => {
    it('renders editable description', async () => {
        const { getByText, getByRole, queryByText } = render(
            <TestPageContainer>
                <SchemaDescriptionField description="test description updated" isEdited onUpdate={async () => {}} />
            </TestPageContainer>,
        );
        expect(getByRole('img')).toBeInTheDocument();
        expect(getByText('test description updated')).toBeInTheDocument();
        expect(queryByText('Update description')).not.toBeInTheDocument();
    });

    it('renders update description modal', async () => {
        const { getByText, getByRole, queryByText } = render(
            <TestPageContainer>
                <SchemaDescriptionField description="test description" isEdited onUpdate={async () => {}} />
            </TestPageContainer>,
        );
        expect(queryByText('Update description')).not.toBeInTheDocument();
        fireEvent.click(getByRole('img'));
        await waitFor(() => expect(getByText('Update description')).toBeInTheDocument());
        expect(getByText('Cancel')).toBeInTheDocument();
        expect(getByText('Update')).toBeInTheDocument();
        expect(getByText('Original:')).toBeInTheDocument();
        fireEvent.click(getByText('Cancel'));
        await waitFor(() => expect(queryByText('Update description')).not.toBeInTheDocument());
    });
});
