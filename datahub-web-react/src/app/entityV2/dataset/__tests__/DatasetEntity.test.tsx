import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';

import { DatasetEntity } from '@app/entityV2/dataset/DatasetEntity';
import TestPageContainer from '@utils/test-utils/TestPageContainer';
import { NEUTRAL, SIXTY_DAYS_AGO, clockColor } from '@utils/test-utils/freshnessClock';

import { Dataset, EntityType, SearchResult } from '@types';

describe('DatasetEntity search card freshness clock', () => {
    it('shows the time of a view in the neutral icon color', () => {
        const dataset = {
            urn: 'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.my_view,PROD)',
            type: EntityType.Dataset,
            name: 'my_view',
            platform: { urn: 'urn:li:dataPlatform:snowflake', type: EntityType.DataPlatform, name: 'snowflake' },
            properties: { name: 'my_view', lastModified: { time: SIXTY_DAYS_AGO } },
            subTypes: { typeNames: ['View'] },
            viewProperties: { materialized: false },
        } as unknown as Dataset;
        render(
            <MockedProvider mocks={[]}>
                <TestPageContainer>
                    {new DatasetEntity().renderSearch({
                        entity: dataset,
                        matchedFields: [],
                    } as unknown as SearchResult)}
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(clockColor()).toBe(NEUTRAL);
    });
});
