import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';

import { PreviewType } from '@app/entityV2/Entity';
import { Preview } from '@app/entityV2/mlFeature/preview/Preview';
import { mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { DataPlatform, EntityType, Health, HealthStatus, HealthStatusType } from '@types';

const FEATURE_URN = 'urn:li:mlFeature:(fraud_features,txn_amount_30d)';
const FEATURE_NAME = 'txn_amount_30d';
const FEATURE_NAMESPACE = 'fraud_features';
const HEALTH_ICON_TEST_ID = `${FEATURE_URN}-health-icon`;

const platform = {
    urn: 'urn:li:dataPlatform:feast',
    type: EntityType.DataPlatform,
    name: 'feast',
} as DataPlatform;

const failingIncidentHealth: Health[] = [
    {
        type: HealthStatusType.Incidents,
        status: HealthStatus.Fail,
        message: '1 active incident',
        causes: [],
    },
];

const renderPreview = (health?: Health[] | null) =>
    render(
        <MockedProvider mocks={mocks} addTypename={false}>
            <TestPageContainer>
                <Preview
                    urn={FEATURE_URN}
                    data={{ name: FEATURE_NAME }}
                    name={FEATURE_NAME}
                    featureNamespace={FEATURE_NAMESPACE}
                    platform={platform}
                    health={health}
                    previewType={PreviewType.PREVIEW}
                />
            </TestPageContainer>
        </MockedProvider>,
    );

describe('Preview', () => {
    it('renders the health icon when the feature has an active incident', () => {
        const { getByText, getByTestId } = renderPreview(failingIncidentHealth);
        expect(getByText(FEATURE_NAME)).toBeInTheDocument();
        expect(getByTestId(HEALTH_ICON_TEST_ID)).toBeInTheDocument();
    });

    it('renders no health icon when health is undefined', () => {
        const { getByText, queryByTestId } = renderPreview(undefined);
        expect(getByText(FEATURE_NAME)).toBeInTheDocument();
        expect(queryByTestId(HEALTH_ICON_TEST_ID)).not.toBeInTheDocument();
    });
});
