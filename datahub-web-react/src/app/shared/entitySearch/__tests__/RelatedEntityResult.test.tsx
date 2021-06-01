import React from 'react';
import { render } from '@testing-library/react';
import { EntityType, PlatformNativeType, SearchResult } from '../../../../types.generated';
import TestPageContainer from '../../../../utils/test-utils/TestPageContainer';
import RelatedEntityResults from '../RelatedEntityResults';

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
                description: 'this is a dataset',
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
                description: 'this is also a dataset',
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

describe('RelatedEntityResults', () => {
    it('renders a menu datasets option', () => {
        const { getByText } = render(
            <TestPageContainer>
                <RelatedEntityResults searchResult={searchResult} />;
            </TestPageContainer>,
        );
        expect(getByText('this is a dataset')).toBeInTheDocument();
    });
});
