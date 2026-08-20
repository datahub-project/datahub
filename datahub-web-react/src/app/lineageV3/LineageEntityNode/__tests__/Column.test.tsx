import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import React from 'react';
import { ReactFlowProvider } from 'reactflow';

import Column from '@app/lineageV3/LineageEntityNode/Column';
import { ColumnAsset, LineageAssetType } from '@app/lineageV3/types';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { EntityType, SchemaFieldDataType } from '@types';

vi.mock('@app/useAppConfig', () => ({
    useAppConfig: () => ({
        config: {
            featureFlags: {
                schemaFieldCLLEnabled: false,
                schemaFieldLineageIgnoreStatus: false,
            },
        },
    }),
}));

vi.mock('@app/lineageV3/utils/lineageUtils', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@app/lineageV3/utils/lineageUtils')>()),
    useGetLineageUrl: () => '/lineage/test',
}));

vi.mock('@app/lineage/utils/useGetLineageTimeParams', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@app/lineage/utils/useGetLineageTimeParams')>()),
    useGetLineageTimeParams: () => ({ startTimeMillis: undefined, endTimeMillis: undefined }),
}));

vi.mock('@graphql/lineage.generated', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@graphql/lineage.generated')>()),
    useGetLineageCountsLazyQuery: () => [vi.fn(), { loading: false }],
}));

function buildColumnAsset(description?: string | null): ColumnAsset {
    return {
        name: 'user_id',
        type: LineageAssetType.Column,
        dataType: SchemaFieldDataType.String,
        nativeDataType: 'varchar',
        description,
    };
}

function renderColumn(lineageAsset: ColumnAsset) {
    return render(
        <MockedProvider mocks={[]}>
            <TestPageContainer>
                <ReactFlowProvider>
                    <Column
                        parentUrn="urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)"
                        entityType={EntityType.Dataset}
                        fieldPath={lineageAsset.name}
                        highlighted={false}
                        hasLineage={false}
                        connectedToHomeNode={false}
                        type={lineageAsset.dataType}
                        nativeDataType={lineageAsset.nativeDataType}
                        lineageAsset={lineageAsset}
                        allNeighborsFetched
                        selectedColumn={null}
                        setSelectedColumn={vi.fn()}
                        hoveredColumn={null}
                        setHoveredColumn={vi.fn()}
                    />
                </ReactFlowProvider>
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('Column description tooltip', () => {
    it('shows the field description on hover', async () => {
        renderColumn(buildColumnAsset('Unique identifier for the user'));

        fireEvent.mouseOver(screen.getByText('user_id'));

        await waitFor(() => {
            expect(screen.getByText('Unique identifier for the user')).toBeInTheDocument();
        });
    });

    it('repeats the column name in the tooltip so a truncated name stays readable', async () => {
        renderColumn(buildColumnAsset('Unique identifier for the user'));

        fireEvent.mouseOver(screen.getByText('user_id'));

        const tooltip = await screen.findByRole('tooltip');
        expect(within(tooltip).getByText('user_id')).toBeInTheDocument();
        expect(within(tooltip).getByText('Unique identifier for the user')).toBeInTheDocument();
    });

    it('renders no tooltip on hover when there is no description', async () => {
        renderColumn(buildColumnAsset(null));

        fireEvent.mouseOver(screen.getByText('user_id'));

        // Guard, not a red-green case: with no description there is nothing to show, and antd's
        // ellipsis tooltip does not fire because the name does not overflow in jsdom.
        await waitFor(() => {
            expect(screen.queryByRole('tooltip')).not.toBeInTheDocument();
        });
    });
});
