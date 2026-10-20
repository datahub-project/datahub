import { act, render, screen } from '@testing-library/react';
import React, { useContext, useEffect } from 'react';
import { describe, expect, it, vi } from 'vitest';

import LineageExplorer from '@app/lineageV3/LineageExplorer';
import { LineageNodesContext } from '@app/lineageV3/common';

import { EntityType } from '@types';

const PRODUCT_URN = 'urn:li:dataProduct:p1';
const MEMBER_URN = 'urn:li:dataset:(urn:li:dataPlatform:mysql,db.member,PROD)';

const mocks = vi.hoisted(() => ({
    startTimeMillis: undefined as number | undefined,
    endTimeMillis: undefined as number | undefined,
}));

vi.mock('@app/lineage/utils/useGetLineageTimeParams', () => ({
    useGetLineageTimeParams: () => ({ startTimeMillis: mocks.startTimeMillis, endTimeMillis: mocks.endTimeMillis }),
}));

vi.mock('@app/lineageV3/common', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@app/lineageV3/common')>()),
    useIgnoreSchemaFieldStatus: () => false,
}));

// Like Apollo: the member query only completes again when its variables change or it remounts.
// Its variables do not include the time range.
vi.mock('@graphql/dataProduct.generated', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@graphql/dataProduct.generated')>()),
    useGetDataProductEntitiesForLineageQuery: ({
        variables,
        onCompleted,
    }: {
        variables: unknown;
        onCompleted: (data: unknown) => void;
    }) => {
        const key = JSON.stringify(variables);
        useEffect(() => {
            onCompleted({
                dataProduct: {
                    entities: {
                        total: 1,
                        searchResults: [{ entity: { urn: MEMBER_URN, type: EntityType.Dataset } }],
                    },
                },
            });
            // eslint-disable-next-line react-hooks/exhaustive-deps
        }, [key]);
    },
}));

vi.mock('@app/lineageV3/LineageDisplay', () => ({
    default: function NodeList() {
        const { nodes, nodeVersion } = useContext(LineageNodesContext);
        return (
            <div data-testid="nodes" data-version={nodeVersion}>
                {Array.from(nodes.keys()).join(',')}
            </div>
        );
    },
}));

describe('LineageExplorer', () => {
    it('re-adds a data product graph members after the time range changes', () => {
        const { rerender } = render(<LineageExplorer urn={PRODUCT_URN} type={EntityType.DataProduct} />);
        expect(screen.getByTestId('nodes').textContent).toContain(MEMBER_URN);

        act(() => {
            mocks.startTimeMillis = 1_600_000_000_000;
            mocks.endTimeMillis = 1_700_000_000_000;
        });
        rerender(<LineageExplorer urn={PRODUCT_URN} type={EntityType.DataProduct} />);

        expect(screen.getByTestId('nodes').textContent).toContain(PRODUCT_URN);
        expect(screen.getByTestId('nodes').textContent).toContain(MEMBER_URN);
    });
});
