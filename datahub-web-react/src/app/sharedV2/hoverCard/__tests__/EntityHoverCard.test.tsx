import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import EntityHoverCard from '@app/sharedV2/hoverCard/EntityHoverCard';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import {
    CorpGroup,
    CorpUser,
    Dataset,
    Entity,
    EntityType,
    GlossaryTerm,
    HealthStatus,
    HealthStatusType,
    OwnershipType,
    Tag,
} from '@types';

const owner = {
    urn: 'urn:li:corpuser:jdoe',
    type: EntityType.CorpUser,
    username: 'jdoe',
    properties: { displayName: 'Jane Doe', title: 'Director of Data Engineering', active: true },
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

    it('shows a job title instead of the type name for a person', () => {
        renderCard(owner);

        expect(screen.getByText('Jane Doe')).toBeInTheDocument();
        expect(screen.getByText('Director of Data Engineering')).toBeInTheDocument();
        expect(screen.queryByText('User')).not.toBeInTheDocument();
    });

    it('shows the ownership role when the hover is opened from an owner', () => {
        render(
            <MockedProvider mocks={[]}>
                <TestPageContainer>
                    <EntityHoverCard
                        entity={owner}
                        ownershipRole={{
                            name: 'Technical Owner',
                            description: 'Responsible for the technical aspects',
                        }}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText('Technical Owner')).toBeInTheDocument();
        expect(screen.getByText('Responsible for the technical aspects')).toBeInTheDocument();
    });

    it('links a glossary term to its related assets', () => {
        renderCard({
            urn: 'urn:li:glossaryTerm:customer-id',
            type: EntityType.GlossaryTerm,
            name: 'Customer ID',
            hierarchicalName: 'Customer ID',
        } as GlossaryTerm);

        expect(screen.getByRole('link', { name: /View Related Assets/ })).toHaveAttribute(
            'href',
            expect.stringContaining('Related%20Assets'),
        );
    });

    it('shows a group member count when the query selected one', () => {
        renderCard({
            urn: 'urn:li:corpGroup:data-platform',
            type: EntityType.CorpGroup,
            name: 'data-platform',
            memberCount: { total: 12 },
        } as unknown as CorpGroup);

        expect(screen.getByText('12 members')).toBeInTheDocument();
    });

    it('keeps clicks inside the card from reaching the row it was opened from', () => {
        const onRowClick = vi.fn();
        render(
            <MockedProvider mocks={[]}>
                <TestPageContainer>
                    {/* eslint-disable-next-line jsx-a11y/click-events-have-key-events, jsx-a11y/no-static-element-interactions */}
                    <div onClick={onRowClick}>
                        <EntityHoverCard entity={dataset} />
                    </div>
                </TestPageContainer>
            </MockedProvider>,
        );

        fireEvent.click(screen.getByText('Owners'));

        expect(onRowClick).not.toHaveBeenCalled();
    });

    it('prefers the edited description over the ingested one', () => {
        renderCard(dataset);

        expect(screen.getByText('Edited description')).toBeInTheDocument();
        expect(screen.queryByText('Ingested description')).not.toBeInTheDocument();
    });
});
