import React from 'react';
import { render } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';

import { EntityType, PlatformNativeType, SearchResult } from '../../../../types.generated';
import TestPageContainer from '../../../../utils/test-utils/TestPageContainer';
import RelatedEntityResults from '../RelatedEntityResults';
import { mocks } from '../../../../Mocks';

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
                platformNativeType: PlatformNativeType.Table,
                properties: {
                    description: 'this is a dataset',
                },
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
                platformNativeType: PlatformNativeType.Table,
                properties: {
                    description: 'this is also a dataset',
                },
                platform: {
                    name: 'kafka',
                },
                tags: [],
            },
            matchedFields: [],
        } as SearchResult,
    ],
};

describe('RelatedEntityResults', () => {
    it('renders a menu datasets option', () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <RelatedEntityResults searchResult={searchResult} />;
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('this is a dataset')).toBeInTheDocument();
    });
});
