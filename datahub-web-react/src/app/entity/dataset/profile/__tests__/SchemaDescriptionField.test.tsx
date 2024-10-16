import React from 'react';
import { fireEvent, render, waitFor } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
import SchemaDescriptionField from '../schema/components/SchemaDescriptionField';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import { mocks } from '../../../../../Mocks';

describe('SchemaDescriptionField', () => {
    it('renders editable description', async () => {
        const { getByText, getByRole, queryByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <SchemaDescriptionField
                        expanded
                        onExpanded={() => {}}
                        description="test description updated"
                        isEdited
                        onUpdate={async () => {}}
                    />{' '}
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByRole('img')).toBeInTheDocument();
        expect(getByText('test description updated')).toBeInTheDocument();
        expect(queryByText('Update description')).not.toBeInTheDocument();
    });

    it('renders update description modal', async () => {
        const { getByText, getByRole, queryByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <SchemaDescriptionField
                        expanded
                        onExpanded={() => {}}
                        description="test description"
                        original="test description"
                        isEdited
                        onUpdate={async () => {}}
                    />
                </TestPageContainer>
            </MockedProvider>,
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
            <SchemaDescriptionField
                expanded
                onExpanded={() => {}}
                description="short description"
                onUpdate={() => Promise.resolve()}
            />,
        );
        expect(getByText('short description')).toBeInTheDocument();
        expect(queryByText('Read Less')).not.toBeInTheDocument();
        expect(queryByText('Read More')).not.toBeInTheDocument();
    });

    describe('renders longer messages with show more / show less', () => {
        const longDescription =
            'really long description over 80 characters, really long description over 80 characters, really long description over 80 characters, really long description over 80 characters, really long description over 80 characters';
        it('renders longer messages with show more when not expanded', () => {
            const onClick = vi.fn();
            const { getByText, queryByText } = render(
                <SchemaDescriptionField
                    expanded={false}
                    onExpanded={onClick}
                    description={longDescription}
                    onUpdate={() => Promise.resolve()}
                />,
            );
            expect(getByText('Read More')).toBeInTheDocument();
            expect(queryByText(longDescription)).not.toBeInTheDocument();
            fireEvent.click(getByText('Read More'));
            expect(onClick).toHaveBeenCalled();
        });

        it('renders longer messages with show less when expanded', () => {
            const onClick = vi.fn();
            const { getByText } = render(
                <SchemaDescriptionField
                    expanded
                    onExpanded={onClick}
                    description={longDescription}
                    onUpdate={() => Promise.resolve()}
                />,
            );
            expect(getByText(longDescription)).toBeInTheDocument();
            expect(getByText('Read Less')).toBeInTheDocument();
            fireEvent.click(getByText('Read Less'));
            expect(onClick).toHaveBeenCalled();
        });
    });
});
