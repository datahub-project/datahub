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
                <SchemaDescriptionField
                    description="test description"
                    original="test description"
                    isEdited
                    onUpdate={async () => {}}
                />
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

    it('renders short messages without show more / show less', () => {
        const { getByText, queryByText } = render(
            <SchemaDescriptionField description="short description" onUpdate={() => Promise.resolve()} />,
        );
        expect(getByText('short description')).toBeInTheDocument();
        expect(queryByText('Read Less')).not.toBeInTheDocument();
        expect(queryByText('Read More')).not.toBeInTheDocument();
    });

    it('renders longer messages with show more / show less', () => {
        const longDescription =
            'really long description over 80 characters, really long description over 80 characters, really long description over 80 characters, really long description over 80 characters, really long description over 80 characters';
        const { getByText, queryByText } = render(
            <SchemaDescriptionField description={longDescription} onUpdate={() => Promise.resolve()} />,
        );
        expect(getByText('Read More')).toBeInTheDocument();
        expect(queryByText(longDescription)).not.toBeInTheDocument();

        fireEvent(
            getByText('Read More'),
            new MouseEvent('click', {
                bubbles: true,
                cancelable: true,
            }),
        );

        expect(getByText(longDescription)).toBeInTheDocument();
        expect(getByText('Read Less')).toBeInTheDocument();

        fireEvent(
            getByText('Read Less'),
            new MouseEvent('click', {
                bubbles: true,
                cancelable: true,
            }),
        );

        expect(getByText('Read More')).toBeInTheDocument();
        expect(queryByText(longDescription)).not.toBeInTheDocument();
    });
});
