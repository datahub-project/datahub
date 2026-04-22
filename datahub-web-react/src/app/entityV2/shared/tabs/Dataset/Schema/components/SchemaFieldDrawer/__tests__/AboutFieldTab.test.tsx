import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import { AboutFieldTab } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/AboutFieldTab';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

vi.mock('@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsTabWrapper', () => ({
    default: ({ properties }: { properties: { fieldProfile?: any } }) =>
        properties.fieldProfile ? <div data-testid="stats-tab-wrapper" /> : null,
}));

vi.mock('@app/entity/shared/EntityContext', () => ({
    useMutationUrn: () => 'urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)',
    useBaseEntity: () => ({}),
    useRouteToTab: () => vi.fn(),
    useEntityData: () => ({ urn: '', entityType: 'DATASET', entityData: null }),
    useRefetch: () => vi.fn(),
}));

vi.mock('@app/shared/hooks/useFileUpload', () => ({
    default: () => ({ uploadFile: vi.fn() }),
}));

vi.mock('@app/shared/hooks/useFileUploadAnalyticsCallbacks', () => ({
    default: () => ({ onFileUploadStart: vi.fn(), onFileUploadSuccess: vi.fn(), onFileUploadError: vi.fn() }),
}));

vi.mock('@src/app/entityV2/shared/sidebarSection/SidebarStructuredProperties', () => ({
    default: () => <div data-testid="sidebar-structured-properties" />,
}));

vi.mock('@app/entityV2/shared/notes/NotesSection', () => ({
    default: () => <div data-testid="notes-section" />,
}));

vi.mock('@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldDetails', () => ({
    FieldDetails: () => <div data-testid="field-details" />,
}));

vi.mock('@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldDescription', () => ({
    default: ({ expandedField }: { expandedField: any }) => (
        <div data-testid="field-description">{expandedField?.fieldPath}</div>
    ),
}));

vi.mock('@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldTags', () => ({
    default: () => <div data-testid="field-tags" />,
}));

vi.mock('@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldTerms', () => ({
    default: () => <div data-testid="field-terms" />,
}));

vi.mock('@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldBusinessAttribute', () => ({
    default: () => <div data-testid="field-business-attribute" />,
}));

const mockSchemaField = {
    fieldPath: 'order_id',
    type: { type: 'NUMBER' },
    nativeDataType: 'BIGINT',
    nullable: false,
    recursive: false,
    schemaFieldEntity: { urn: 'urn:li:schemaField:(urn:li:dataset:test,order_id)', deprecation: null },
} as any;

const mockFieldProfile = {
    fieldPath: 'order_id',
    nullCount: 0,
    nullProportion: 0.0,
    uniqueCount: 100,
    uniqueProportion: 1.0,
} as any;

const baseProperties = {
    schemaFields: [mockSchemaField],
    expandedDrawerFieldPath: 'order_id',
    editableSchemaMetadata: null,
    usageStats: null,
    fieldProfile: mockFieldProfile,
    profiles: [],
    fetchDataWithLookbackWindow: vi.fn(),
    profilesDataLoading: false,
    notes: [],
    setSelectedTabName: vi.fn(),
    refetch: vi.fn(),
    refetchNotes: vi.fn(),
    fieldUrn: 'urn:li:schemaField:(urn:li:dataset:test,order_id)',
    assetUrn: 'urn:li:dataset:test',
};

describe('AboutFieldTab', () => {
    it('renders full stats content in About tab when field profile exists', () => {
        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <AboutFieldTab properties={baseProperties} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByTestId('stats-tab-wrapper')).toBeInTheDocument();
    });

    it('does not render "View all" stats link in About tab', () => {
        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <AboutFieldTab properties={baseProperties} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.queryByText('View all')).not.toBeInTheDocument();
    });

    it('does not render stats content when field profile is undefined', () => {
        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <AboutFieldTab properties={{ ...baseProperties, fieldProfile: undefined }} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.queryByTestId('stats-tab-wrapper')).not.toBeInTheDocument();
    });

    it('renders field description alongside stats when profile exists', () => {
        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <AboutFieldTab properties={baseProperties} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByTestId('field-description')).toBeInTheDocument();
        expect(screen.getByTestId('stats-tab-wrapper')).toBeInTheDocument();
    });
});
