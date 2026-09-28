import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it } from 'vitest';

import EntityHoverCard from '@app/sharedV2/hoverCard/EntityHoverCard';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { CorpUser, Dataset, Entity, EntityType, HealthStatus, HealthStatusType, OwnershipType, Tag } from '@types';

const owner = {
    urn: 'urn:li:corpuser:jdoe',
    type: EntityType.CorpUser,
    username: 'jdoe',
    properties: { displayName: 'Jane Doe', active: true },
} as CorpUser;

const tag = {
    urn: 'urn:li:tag:Certified',
    type: EntityType.Tag,
    name: 'Certified',
    properties: { name: 'Certified', colorHex: '#2F855A' },
} as Tag;

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)';

const dataset = {
    urn: DATASET_URN,
    type: EntityType.Dataset,
    name: 'db.schema.orders',
    platform: { urn: 'urn:li:dataPlatform:snowflake', type: EntityType.DataPlatform, name: 'snowflake' },
    properties: { name: 'orders', description: 'Ingested description' },
    editableProperties: { description: 'Edited description' },
    ownership: { owners: [{ owner, type: OwnershipType.Dataowner, associatedUrn: DATASET_URN }] },
    globalTags: { tags: [{ tag, associatedUrn: DATASET_URN }] },
    health: [{ type: HealthStatusType.Assertions, status: HealthStatus.Fail, message: 'failing', causes: [] }],
    statsSummary: { queryCountLast30Days: 42 },
    upstream: { total: 3, filtered: 0, relationships: [] },
} as unknown as Dataset;

function renderCard(entity: Entity) {
    return render(
        <MockedProvider mocks={[]}>
            <TestPageContainer>
                <EntityHoverCard entity={entity} />
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('EntityHoverCard', () => {
    it('renders only the header for an entity with nothing else to show', () => {
        renderCard(tag);

        expect(screen.getByText('Certified')).toBeInTheDocument();
        expect(screen.queryByText('Owners')).not.toBeInTheDocument();
        expect(screen.queryByText('Documentation')).not.toBeInTheDocument();
    });

    it('renders sections, status badges, and the usage footer when the data is present', () => {
        renderCard(dataset);

        expect(screen.getByText('Owners')).toBeInTheDocument();
        expect(screen.getByText('Jane Doe')).toBeInTheDocument();
        expect(screen.getByText('Tags')).toBeInTheDocument();
        expect(screen.getByTestId(`${DATASET_URN}-health-icon`)).toBeInTheDocument();
        expect(screen.getByText(/42/)).toBeInTheDocument();
    });

    it('prefers the edited description over the ingested one', () => {
        renderCard(dataset);

        expect(screen.getByText('Edited description')).toBeInTheDocument();
        expect(screen.queryByText('Ingested description')).not.toBeInTheDocument();
    });
});
