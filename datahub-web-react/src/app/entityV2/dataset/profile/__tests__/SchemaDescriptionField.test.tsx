import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, waitFor } from '@testing-library/react';
import React from 'react';

import SchemaDescriptionField from '@app/entityV2/dataset/profile/schema/components/SchemaDescriptionField';
import CustomThemeProvider from '@src/CustomThemeProvider';
import { mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

describe('SchemaDescriptionField', () => {
    // Mock IntersectionObserver
    beforeAll(() => {
        class MockIntersectionObserver {
            observe() {}

            unobserve() {}

            disconnect() {}
        }
        (window as any).IntersectionObserver = MockIntersectionObserver;
    });

    afterAll(() => {
        delete (window as any).IntersectionObserver;
    });

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
    }, 10_000);

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
        expect(getByText('Publish')).toBeInTheDocument();
        expect(getByText('Original:')).toBeInTheDocument();
        fireEvent.click(getByText('Cancel'));
        await waitFor(() => expect(queryByText('Update description')).not.toBeInTheDocument());
    });

    it('renders short messages without show more / show less', () => {
        const { getByText, queryByText } = render(
            <CustomThemeProvider>
                <SchemaDescriptionField
                    expanded
                    onExpanded={() => {}}
                    description="short description"
                    onUpdate={() => Promise.resolve()}
                />
            </CustomThemeProvider>,
        );
        expect(getByText('short description')).toBeInTheDocument();
        expect(queryByText('Read Less')).not.toBeInTheDocument();
        expect(queryByText('Read More')).not.toBeInTheDocument();
    });
});
