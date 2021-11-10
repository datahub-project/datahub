import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';
import { mocks } from '../../../../../../Mocks';
import { EntityType } from '../../../../../../types.generated';
import TestPageContainer from '../../../../../../utils/test-utils/TestPageContainer';
import EntityContext from '../../../EntityContext';
import { DocumentationTab } from '../DocumentationTab';

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
                            updateEntity: jest.fn(),
                            routeToTab: jest.fn(),
                        }}
                    >
                        <DocumentationTab />
                    </EntityContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('This is a description')).toBeInTheDocument();
    });

    it('if editable is present, renders edited description', async () => {
        const { getByText, queryByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/dataset/urn:li:dataset:3']}>
                    <EntityContext.Provider
                        value={{
                            urn: 'urn:li:dataset:123',
                            entityType: EntityType.Dataset,
                            entityData: {
                                description: 'This is a description',
                                editableProperties: {
                                    description: 'Edited description',
                                },
                            },
                            baseEntity: {},
                            updateEntity: jest.fn(),
                            routeToTab: jest.fn(),
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
