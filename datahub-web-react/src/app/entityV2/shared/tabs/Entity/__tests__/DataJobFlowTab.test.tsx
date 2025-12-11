/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';

import { EntityContext } from '@app/entity/shared/EntityContext';
import { getDataForEntityType } from '@app/entityV2/shared/containers/profile/utils';
import { DataJobFlowTab } from '@app/entityV2/shared/tabs/Entity/DataJobFlowTab';
import { dataJob1, mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { EntityType } from '@types';

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
