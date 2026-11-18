import { render, screen, waitFor } from '@testing-library/react';
import React, { useContext, useEffect } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import LineageExplorer from '@app/lineageV3/LineageExplorer';
import { BOUNDING_BOX_MEMBER_PAGE_SIZE, LineageNodesContext, NodeContext } from '@app/lineageV3/common';

import { EntityType } from '@types';

const PRODUCT_URN = 'urn:li:dataProduct:p1';
const memberUrn = (start: number) => `urn:li:dataset:(urn:li:dataPlatform:mysql,db.member_${start},PROD)`;

const mocks = vi.hoisted(() => ({
    startTimeMillis: undefined as number | undefined,
    endTimeMillis: undefined as number | undefined,
    nodes: undefined as NodeContext['nodes'] | undefined,
}));

vi.mock('@app/lineageV3/utils/useGetLineageTimeParams', () => ({
    useGetLineageTimeParams: () => ({ startTimeMillis: mocks.startTimeMillis, endTimeMillis: mocks.endTimeMillis }),
}));

vi.mock('@app/lineageV3/common', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@app/lineageV3/common')>()),
    useIgnoreSchemaFieldStatus: () => false,
}));

// Like Apollo: the member query only completes again when its variables change or it remounts, its
// variables don't include the time range, and a request superseded by new variables never completes.
// Each page holds one member, named after its start offset.
vi.mock('@graphql/dataProduct.generated', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@graphql/dataProduct.generated')>()),
    useGetDataProductEntitiesForLineageQuery: ({
        variables,
        onCompleted,
    }: {
        variables: { start: number };
        onCompleted: (data: unknown) => void;
    }) => {
        const key = JSON.stringify(variables);
        useEffect(() => {
            const timeout = setTimeout(() =>
                onCompleted({
                    dataProduct: {
                        entities: {
                            total: 3 * BOUNDING_BOX_MEMBER_PAGE_SIZE,
                            searchResults: [{ entity: { urn: memberUrn(variables.start), type: EntityType.Dataset } }],
                        },
                    },
                }),
            );
            return () => clearTimeout(timeout);
            // eslint-disable-next-line react-hooks/exhaustive-deps
        }, [key]);
    },
}));

vi.mock('@app/lineageV3/LineageDisplay', () => ({
    default: function NodeList() {
        const { nodes, nodeVersion } = useContext(LineageNodesContext);
        mocks.nodes = nodes;
        return (
            <div data-testid="nodes" data-version={nodeVersion}>
                {Array.from(nodes.keys()).join(',')}
            </div>
        );
    },
}));

function changeTimeRange(rerender: (ui: React.ReactElement) => void) {
    mocks.startTimeMillis = 1_600_000_000_000;
    mocks.endTimeMillis = 1_700_000_000_000;
    rerender(<LineageExplorer urn={PRODUCT_URN} type={EntityType.DataProduct} />);
}

describe('LineageExplorer', () => {
    beforeEach(() => {
        mocks.startTimeMillis = undefined;
        mocks.endTimeMillis = undefined;
    });

    it('re-adds a data product graph members after the time range changes', async () => {
        const { rerender } = render(<LineageExplorer urn={PRODUCT_URN} type={EntityType.DataProduct} />);
        await waitFor(() => expect(screen.getByTestId('nodes').textContent).toContain(memberUrn(0)));

        changeTimeRange(rerender);

        await waitFor(() => expect(screen.getByTestId('nodes').textContent).toContain(memberUrn(0)));
        expect(screen.getByTestId('nodes').textContent).toContain(PRODUCT_URN);
    });

    it('refetches the first member page after a time-range change, even after "Show more"', async () => {
        const { rerender } = render(<LineageExplorer urn={PRODUCT_URN} type={EntityType.DataProduct} />);
        await waitFor(() => expect(screen.getByTestId('nodes').textContent).toContain(memberUrn(0)));
        const homeNode = mocks.nodes?.get(PRODUCT_URN);
        if (homeNode) homeNode.boundingBoxLimit = 2 * BOUNDING_BOX_MEMBER_PAGE_SIZE;

        changeTimeRange(rerender);

        await waitFor(() => expect(screen.getByTestId('nodes').textContent).toContain(memberUrn(0)));
    });
});
