import React from 'react';
import { render } from '@testing-library/react';
import { EntityType, PlatformNativeType } from '../../../../types.generated';
import TestPageContainer from '../../../../utils/test-utils/TestPageContainer';
import RelatedEntityResults from '../RelatedEntityResults';

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

describe('RelatedEntityResults', () => {
    it('renders a menu datasets option', () => {
        const { getByText } = render(
            <TestPageContainer>
                <RelatedEntityResults searchResult={searchResult} />;
            </TestPageContainer>,
        );
        expect(getByText('Datasets')).toBeInTheDocument();
    });

    it('will  show the related dataset when selected', () => {
        const { getByText } = render(
            <TestPageContainer>
                <RelatedEntityResults searchResult={searchResult} />;
            </TestPageContainer>,
        );
        expect(getByText('Related Datasets')).toBeInTheDocument();
    });
});
