import React from 'react';
import { render } from '@testing-library/react';
import { EntityType, PlatformNativeType } from '../../../../types.generated';
import TestPageContainer from '../../../../utils/test-utils/TestPageContainer';
import RelatedEntity from '../RelatedEntity';

const searchResult = {
    [EntityType.Dataset]: [
        {
            entity: {
                name: 'HiveDataset',
                origin: 'PROD',
                description: 'this is a dataset',
                platformNativeType: PlatformNativeType.Table,
                platform: {
                    name: 'hive',
                },
                tags: [],
            },
        },
        {
            entity: {
                name: 'KafkaDataset',
                origin: 'PROD',
                description: 'this is also a dataset',
                platformNativeType: PlatformNativeType.Table,
                platform: {
                    name: 'kafka',
                },
                tags: [],
            },
        },
    ],
};

describe('RelatedEntity', () => {
    it('renders a list container', () => {
        const { getByText } = render(
            <TestPageContainer>
                <RelatedEntity searchResult={searchResult} entityPath="dataset" />
            </TestPageContainer>,
        );
        expect(getByText('Related Datasets')).toBeInTheDocument();
    });

    it('renders the entity rows', () => {
        const { getByText } = render(
            <TestPageContainer>
                <RelatedEntity searchResult={searchResult} entityPath="dataset" />
            </TestPageContainer>,
        );
        expect(getByText('this is a dataset')).toBeInTheDocument();
        expect(getByText('this is also a dataset')).toBeInTheDocument();
    });
});
