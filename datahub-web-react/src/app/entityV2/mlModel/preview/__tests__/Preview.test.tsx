import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';

import { PreviewType } from '@app/entityV2/Entity';
import { Preview } from '@app/entityV2/mlModel/preview/Preview';
import { mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { EntityType, Health, HealthStatus, HealthStatusType, MlModel } from '@types';

const MODEL_URN = 'urn:li:mlModel:(urn:li:dataPlatform:sagemaker,trustmodel,PROD)';
const MODEL_NAME = 'trust model';
const HEALTH_ICON_TEST_ID = `${MODEL_URN}-health-icon`;

const model = {
    urn: MODEL_URN,
    type: EntityType.Mlmodel,
    name: MODEL_NAME,
    platform: {
        urn: 'urn:li:dataPlatform:sagemaker',
        type: EntityType.DataPlatform,
        name: 'sagemaker',
    },
} as MlModel;

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
                <Preview data={{ name: MODEL_NAME }} model={model} health={health} previewType={PreviewType.PREVIEW} />
            </TestPageContainer>
        </MockedProvider>,
    );

describe('Preview', () => {
    it('renders the health icon when the model has an active incident', () => {
        const { getByText, getByTestId } = renderPreview(failingIncidentHealth);
        expect(getByText(MODEL_NAME)).toBeInTheDocument();
        expect(getByTestId(HEALTH_ICON_TEST_ID)).toBeInTheDocument();
    });

    it('renders no health icon when health is undefined', () => {
        const { getByText, queryByTestId } = renderPreview(undefined);
        expect(getByText(MODEL_NAME)).toBeInTheDocument();
        expect(queryByTestId(HEALTH_ICON_TEST_ID)).not.toBeInTheDocument();
    });
});
