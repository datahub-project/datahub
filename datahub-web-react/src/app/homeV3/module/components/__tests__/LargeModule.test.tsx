import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';

import { DataHubPageModuleType, EntityType, PageModuleScope } from '@types';

// Mock styled-components
vi.mock('styled-components', () => {
    const styledFactory = (Component: any) => () => {
        return ({ children, ...props }: any) => <Component {...props}>{children}</Component>;
    };
    styledFactory.div = styledFactory('div');
    styledFactory.button = styledFactory('button');
    styledFactory.Icon = styledFactory(() => null);
    return {
        __esModule: true,
        default: styledFactory,
    };
});

// Mock child components
vi.mock('@app/homeV3/module/components/ModuleContainer', () => ({
    default: ({ children, ...props }: any) => (
        <div data-testid="module-container" {...props}>
            {children}
        </div>
    ),
}));

vi.mock('@app/homeV3/module/components/ModuleMenu', () => ({
    default: () => <div data-testid="module-menu">Module Menu</div>,
}));

vi.mock('@app/homeV3/module/components/ModuleName', () => ({
    default: ({ text }: { text: string }) => <div data-testid="module-name">{text}</div>,
}));

vi.mock('@components', () => ({
    Button: ({ children, onClick, ...props }: any) => (
        <button type="button" onClick={onClick} {...props}>
            {children}
        </button>
    ),
    Loader: () => <div data-testid="loader">Loading...</div>,
    Text: ({ children }: any) => <span>{children}</span>,
    Icon: () => <svg />,
    borders: {
        '1px': '1px solid',
    },
    colors: {
        white: '#ffffff',
        gray: {
            100: '#f3f4f6',
        },
    },
    radius: {
        lg: '8px',
    },
    spacing: {
        md: '16px',
        xsm: '4px',
    },
}));

describe('LargeModule', () => {
    const mockModule: ModuleProps['module'] = {
        urn: 'urn:li:dataHubPageModule:test',
        type: EntityType.DatahubPageModule,
        properties: {
            name: 'Test Module',
            type: DataHubPageModuleType.OwnedAssets,
            visibility: {
                scope: PageModuleScope.Global,
            },
            params: {},
        },
    };

    const defaultProps = {
        module: mockModule,
        children: <div data-testid="module-content">Module Content</div>,
        position: { rowIndex: 0, moduleIndex: 0 },
    };

    it('should render the module with correct name', () => {
        render(<LargeModule {...defaultProps} />);

        expect(screen.getByTestId('module-name')).toHaveTextContent('Test Module');
        expect(screen.getByTestId('module-content')).toBeInTheDocument();
    });

    it('should render view all button when onClickViewAll is provided', () => {
        const mockOnClickViewAll = vi.fn();
        render(<LargeModule {...defaultProps} onClickViewAll={mockOnClickViewAll} />);

        const viewAllButton = screen.getByTestId('view-all');
        expect(viewAllButton).toBeInTheDocument();
        expect(viewAllButton).toHaveTextContent('View all');
    });

    it('should not render view all button when onClickViewAll is not provided', () => {
        render(<LargeModule {...defaultProps} />);

        expect(screen.queryByTestId('view-all')).not.toBeInTheDocument();
    });

    it('should call onClickViewAll when view all button is clicked', () => {
        const mockOnClickViewAll = vi.fn();
        render(<LargeModule {...defaultProps} onClickViewAll={mockOnClickViewAll} />);

        const viewAllButton = screen.getByTestId('view-all');
        viewAllButton.click();

        expect(mockOnClickViewAll).toHaveBeenCalledTimes(1);
    });

    it('should render loader when loading is true', () => {
        render(<LargeModule {...defaultProps} loading />);

        expect(screen.getByTestId('loader')).toBeInTheDocument();
        expect(screen.queryByTestId('module-content')).not.toBeInTheDocument();
    });

    it('should render children when loading is false', () => {
        render(<LargeModule {...defaultProps} loading={false} />);

        expect(screen.getByTestId('module-content')).toBeInTheDocument();
        expect(screen.queryByTestId('loader')).not.toBeInTheDocument();
    });

    it('should render children when loading is not provided', () => {
        render(<LargeModule {...defaultProps} />);

        expect(screen.getByTestId('module-content')).toBeInTheDocument();
        expect(screen.queryByTestId('loader')).not.toBeInTheDocument();
    });

    it('should render module menu in header', () => {
        render(<LargeModule {...defaultProps} />);

        expect(screen.getByTestId('module-menu')).toBeInTheDocument();
    });

    it('should handle onClickViewAll as undefined', () => {
        render(<LargeModule {...defaultProps} onClickViewAll={undefined} />);

        expect(screen.queryByTestId('view-all-button')).not.toBeInTheDocument();
    });

    it('should handle onClickViewAll as null', () => {
        render(<LargeModule {...defaultProps} onClickViewAll={null as any} />);

        expect(screen.queryByTestId('view-all-button')).not.toBeInTheDocument();
    });
});
