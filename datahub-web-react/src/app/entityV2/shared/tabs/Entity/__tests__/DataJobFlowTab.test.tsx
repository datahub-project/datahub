import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';
import { dataJob1, mocks } from '../../../../../../Mocks';
import { EntityType } from '../../../../../../types.generated';
import TestPageContainer from '../../../../../../utils/test-utils/TestPageContainer';
import { getDataForEntityType } from '../../../containers/profile/utils';
import { EntityContext } from '../../../../../entity/shared/EntityContext';
import { DataJobFlowTab } from '../DataJobFlowTab';

describe('DataJobFlowTab', () => {
    it('renders fields', async () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/dataset/urn:li:dataset:3']}>
                    <EntityContext.Provider
                        value={{
                            urn: 'urn:li:dataJob:1',
                            entityType: EntityType.DataJob,
                            entityData: getDataForEntityType({
                                data: dataJob1,
                                entityType: EntityType.DataJob,
                                getOverrideProperties: () => ({}),
                            }),
                            baseEntity: { dataJob: dataJob1 },
                            updateEntity: vi.fn(),
                            routeToTab: vi.fn(),
                            refetch: vi.fn(),
                            lineage: undefined,
                            loading: true,
                            dataNotCombinedWithSiblings: null,
                        }}
                    >
                        <DataJobFlowTab />
                    </EntityContext.Provider>
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('DataFlowInfoName')).toBeInTheDocument();
        expect(getByText('DataFlowInfo1 Description')).toBeInTheDocument();
    });
});
