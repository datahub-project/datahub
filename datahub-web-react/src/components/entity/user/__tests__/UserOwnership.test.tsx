import React from 'react';
import { render } from '@testing-library/react';
import UserOwnership from '../UserOwnership';
import { EntityType, PlatformNativeType } from '../../../../types.generated';
import TestPageContainer from '../../../../utils/test-utils/TestPageContainer';

const ownerships = {
    [EntityType.Dataset]: [
        {
            name: 'HiveDataset',
            origin: 'PROD',
            description: 'this is a dataset',
            platformNativeType: PlatformNativeType.Table,
        },
        {
            name: 'KafkaDataset',
            origin: 'PROD',
            description: 'this is also a dataset',
            platformNativeType: PlatformNativeType.Table,
        },
    ],
};

describe('UserOwnership', () => {
    it('renders a list container', () => {
        const { getByText } = render(
            <TestPageContainer>
                <UserOwnership ownerships={ownerships} entityPath="dataset" />
            </TestPageContainer>,
        );
        expect(getByText('Datasets they own')).toBeInTheDocument();
    });

    it('renders the entity rows', () => {
        const { getByText } = render(
            <TestPageContainer>
                <UserOwnership ownerships={ownerships} entityPath="dataset" />
            </TestPageContainer>,
        );
        expect(getByText('this is a dataset')).toBeInTheDocument();
        expect(getByText('this is also a dataset')).toBeInTheDocument();
    });
});
