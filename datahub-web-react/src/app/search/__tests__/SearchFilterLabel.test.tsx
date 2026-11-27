import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it } from 'vitest';

import { SearchFilterLabel } from '@app/search/SearchFilterLabel';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { Entity, EntityType } from '@types';

const APPLICATION_URN = 'urn:li:application:billing';

describe('SearchFilterLabel', () => {
    it('labels an entity facet value without a dedicated renderer by the entity name', () => {
        const application = {
            urn: APPLICATION_URN,
            type: EntityType.Application,
            properties: { name: 'Billing App' },
        } as unknown as Entity;

        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <SearchFilterLabel field="applications" value={APPLICATION_URN} entity={application} count={3} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText(/Billing App/)).toBeInTheDocument();
        expect(screen.queryByText(new RegExp(APPLICATION_URN))).not.toBeInTheDocument();
    });

    it('falls back to the raw value when there is no entity', () => {
        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <SearchFilterLabel field="origin" value="PROD" count={3} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText(/PROD/)).toBeInTheDocument();
    });
});
