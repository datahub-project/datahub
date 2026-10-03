import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';

import { PreviewType } from '@app/entityV2/Entity';
import { Preview } from '@app/entityV2/mlFeatureTable/preview/Preview';
import { mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { Health, HealthStatus, HealthStatusType } from '@types';

const FEATURE_TABLE_URN = 'urn:li:mlFeatureTable:(urn:li:dataPlatform:feast,fraud_features)';
const FEATURE_TABLE_NAME = 'fraud_features';
const PLATFORM_NAME = 'Feast';
const HEALTH_ICON_TEST_ID = `${FEATURE_TABLE_URN}-health-icon`;

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
                    urn={FEATURE_TABLE_URN}
                    data={{ name: FEATURE_TABLE_NAME }}
                    name={FEATURE_TABLE_NAME}
                    platformName={PLATFORM_NAME}
                    health={health}
                    previewType={PreviewType.PREVIEW}
                />
            </TestPageContainer>
        </MockedProvider>,
    );

describe('Preview', () => {
    it('renders the health icon when the feature table has an active incident', () => {
        const { getByText, getByTestId } = renderPreview(failingIncidentHealth);
        expect(getByText(FEATURE_TABLE_NAME)).toBeInTheDocument();
        expect(getByTestId(HEALTH_ICON_TEST_ID)).toBeInTheDocument();
    });

    it('renders no health icon when health is undefined', () => {
        const { getByText, queryByTestId } = renderPreview(undefined);
        expect(getByText(FEATURE_TABLE_NAME)).toBeInTheDocument();
        expect(queryByTestId(HEALTH_ICON_TEST_ID)).not.toBeInTheDocument();
    });
});
