import { render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import OutputPortsModule from '@app/entityV2/summary/modules/outputPorts/OutputPortsModule';
import { useGetOutputPorts } from '@app/entityV2/summary/modules/outputPorts/useGetOutputPorts';
import { ModuleProps } from '@app/homeV3/module/types';

import { DataHubPageModuleType, EntityType, PageModuleScope } from '@types';

vi.mock('@components', () => ({
    InfiniteScrollList: ({ totalItemCount }: { totalItemCount: number }) => (
        <div data-testid="infinite-scroll-list" data-total-item-count={totalItemCount} />
    ),
}));

vi.mock('@app/entityV2/summary/modules/outputPorts/useGetOutputPorts', () => ({
    useGetOutputPorts: vi.fn(),
}));

vi.mock('@app/homeV3/module/components/EntityItem', () => ({
    default: () => <div data-testid="entity-item" />,
}));

vi.mock('@app/homeV3/module/components/EmptyContent', () => ({
    default: ({ title, description }: { title: string; description: string }) => (
        <div data-testid="empty-content">
            <h3>{title}</h3>
            <p>{description}</p>
        </div>
    ),
}));

vi.mock('@app/homeV3/module/components/LargeModule', () => ({
    default: ({ children, loading, dataTestId }: any) => (
        <div data-testid={dataTestId} data-loading={loading}>
            {children}
        </div>
    ),
}));

describe('OutputPortsModule', () => {
    const module: ModuleProps['module'] = {
        urn: 'urn:li:dataHubPageModule:output_ports',
        type: EntityType.DatahubPageModule,
        properties: {
            name: 'Output Ports',
            type: DataHubPageModuleType.OutputPorts,
            visibility: { scope: PageModuleScope.Global },
            params: {},
        },
    };

    const props: ModuleProps = {
        module,
        position: { rowIndex: 0, moduleIndex: 0, numberOfModulesInRow: 1 },
    };

    beforeEach(() => {
        vi.clearAllMocks();
        (useGetOutputPorts as unknown as any).mockReturnValue({
            loading: false,
            total: 2,
            fetchOutputPorts: vi.fn(),
        });
    });

    it('returns null when there are no output ports', () => {
        (useGetOutputPorts as unknown as any).mockReturnValueOnce({
            loading: false,
            total: 0,
            fetchOutputPorts: vi.fn(),
        });

        const { container } = render(<OutputPortsModule {...props} />);

        expect(container.firstChild).toBeNull();
    });

    it('renders the large module when output ports exist', () => {
        render(<OutputPortsModule {...props} />);

        expect(screen.getByTestId('output-ports-module')).toBeInTheDocument();
        expect(screen.getByTestId('output-ports-module')).toHaveAttribute('data-loading', 'false');
        expect(screen.getByTestId('infinite-scroll-list')).toHaveAttribute('data-total-item-count', '2');
    });

    it('passes the loading state through to the large module', () => {
        (useGetOutputPorts as unknown as any).mockReturnValueOnce({
            loading: true,
            total: 2,
            fetchOutputPorts: vi.fn(),
        });

        render(<OutputPortsModule {...props} />);

        expect(screen.getByTestId('output-ports-module')).toHaveAttribute('data-loading', 'true');
    });
});
