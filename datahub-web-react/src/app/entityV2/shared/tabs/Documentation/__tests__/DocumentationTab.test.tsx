import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import DOMPurify from 'dompurify';
import React from 'react';

import { EntityContext } from '@app/entity/shared/EntityContext';
import { DocumentationTab } from '@app/entityV2/shared/tabs/Documentation/DocumentationTab';
import { mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { EntityType } from '@types';

describe('SchemaDescriptionField', () => {
    it('renders original description', async () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/dataset/urn:li:dataset:3']}>
                    <EntityContext.Provider
                        value={{
                            urn: 'urn:li:dataset:123',
                            entityType: EntityType.Dataset,
                            entityData: {
                                properties: {
                                    description: 'This is a description',
                                },
                            },
                            baseEntity: {},
                            updateEntity: vi.fn(),
                            routeToTab: vi.fn(),
                            loading: true,
                            lineage: undefined,
                            dataNotCombinedWithSiblings: null,
                            refetch: vi.fn(),
                        }}
                    >
                        <DocumentationTab />
                    </EntityContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('This is a description')).toBeInTheDocument();
    }, 30_000);

    it('if editable is present, renders edited description', async () => {
        const { getByText, queryByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/dataset/urn:li:dataset:3']}>
                    <EntityContext.Provider
                        value={{
                            urn: 'urn:li:dataset:123',
                            entityType: EntityType.Dataset,
                            entityData: {
                                properties: {
                                    description: 'This is a description',
                                },
                                editableProperties: {
                                    description: 'Edited description',
                                },
                            },
                            baseEntity: {},
                            updateEntity: vi.fn(),
                            routeToTab: vi.fn(),
                            refetch: vi.fn(),
                            loading: true,
                            lineage: undefined,
                            dataNotCombinedWithSiblings: null,
                        }}
                    >
                        <DocumentationTab />
                    </EntityContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('Edited description')).toBeInTheDocument();
        expect(queryByText('This is a description')).not.toBeInTheDocument();
    });
});

describe('permission-aware edit affordances', () => {
    const renderTab = (canEditDescription: boolean, entityData: Record<string, unknown>) =>
        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/dataset/urn:li:dataset:3']}>
                    <EntityContext.Provider
                        value={{
                            urn: 'urn:li:dataset:123',
                            entityType: EntityType.Dataset,
                            entityData: {
                                ...entityData,
                                privileges: { canEditDescription },
                            },
                            baseEntity: {},
                            updateEntity: vi.fn(),
                            routeToTab: vi.fn(),
                            loading: true,
                            lineage: undefined,
                            dataNotCombinedWithSiblings: null,
                            refetch: vi.fn(),
                        }}
                    >
                        <DocumentationTab />
                    </EntityContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );

    it('enables the edit button and shows no tooltip when the user has canEditDescription', async () => {
        renderTab(true, { properties: { description: 'This is a description' } });

        const editButton = screen.getByTestId('edit-documentation-button');
        expect(editButton).not.toBeDisabled();

        await userEvent.hover(editButton);
        expect(screen.queryByText('You do not have permission to change this.')).not.toBeInTheDocument();
    });

    it('disables the edit button and shows a tooltip when the user lacks canEditDescription', async () => {
        renderTab(false, { properties: { description: 'This is a description' } });

        const editButton = screen.getByTestId('edit-documentation-button');
        expect(editButton).toBeDisabled();

        await userEvent.hover(editButton);
        expect(await screen.findByText('You do not have permission to change this.')).toBeInTheDocument();
    });

    it('enables the add-documentation button when the user has canEditDescription', async () => {
        renderTab(true, {});

        const addButton = screen.getByTestId('add-documentation');
        expect(addButton).not.toBeDisabled();

        await userEvent.hover(addButton);
        expect(screen.queryByText('You do not have permission to change this.')).not.toBeInTheDocument();
    });

    it('disables the add-documentation button and shows a tooltip when the user lacks canEditDescription', async () => {
        renderTab(false, {});

        const addButton = screen.getByTestId('add-documentation');
        expect(addButton).toBeDisabled();

        await userEvent.hover(addButton);
        expect(await screen.findByText('You do not have permission to change this.')).toBeInTheDocument();
    });
});

describe('markdown sanitization', () => {
    it('should remove malicious tags like <script> from text', () => {
        const text = 'Testing this out<script>console.log("testing")</script>';
        const sanitizedText = DOMPurify.sanitize(text);

        expect(sanitizedText).toBe('Testing this out');
    });

    it('should allow acceptable html', () => {
        const text = '<strong>Testing</strong> this <p>out</p> <span>for</span> <div>safety</div>';
        const sanitizedText = DOMPurify.sanitize(text);

        expect(sanitizedText).toBe(text);
    });

    it('should allow acceptable markdown', () => {
        const text =
            '~~Testing~~ **this** *out* \n\n> for\n\n- safety\n\n1. ordered list\n\n[ test link](https://www.google.com/)\n';
        const sanitizedText = DOMPurify.sanitize(text);

        expect(sanitizedText).toBe(text);
    });
});
