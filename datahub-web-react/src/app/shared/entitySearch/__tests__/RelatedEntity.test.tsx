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

import RelatedEntity from '@app/shared/entitySearch/RelatedEntity';
import { mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { EntityType, PlatformNativeType, SearchResult } from '@types';

const searchResult: {
    [key in EntityType]?: Array<SearchResult>;
} = {
    [EntityType.Dataset]: [
        {
            entity: {
                urn: 'some:urn1',
                type: EntityType.Dataset,
                name: 'HiveDataset',
                origin: 'PROD',
                properties: {
                    description: 'this is a dataset',
                },
                platformNativeType: PlatformNativeType.Table,
                platform: {
                    name: 'hive',
                },
                tags: [],
            },
            matchedFields: [],
        } as SearchResult,
        {
            entity: {
                urn: 'some:urn2',
                type: EntityType.Dataset,
                name: 'KafkaDataset',
                origin: 'PROD',
                properties: {
                    description: 'this is also a dataset',
                },
                platformNativeType: PlatformNativeType.Table,
                platform: {
                    name: 'kafka',
                },
                tags: [],
            },
            matchedFields: [],
        } as SearchResult,
    ],
};

describe('RelatedEntity', () => {
    it('renders the entity rows', () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <RelatedEntity searchResult={searchResult} entityPath="dataset" />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('this is a dataset')).toBeInTheDocument();
        expect(getByText('this is also a dataset')).toBeInTheDocument();
    });
});
