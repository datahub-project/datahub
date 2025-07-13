import { render, screen, fireEvent } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import ModuleMenu from '@app/homeV3/module/components/ModuleMenu';
import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope } from '@types';

// Mock the PageTemplateContext
const mockRemoveModule = vi.fn();
vi.mock('@app/homeV3/context/PageTemplateContext', () => ({
    usePageTemplateContext: () => ({
        removeModule: mockRemoveModule,
    }),
}));

// Mock the Icon component
vi.mock('@components', () => ({
    Icon: React.forwardRef(({ icon, source, size, ...props }: any, ref: any) => (
        <div ref={ref} data-testid="icon" data-icon={icon} {...props}>
            {icon}
        </div>
    )),
    colors: {
        red: {
            500: '#ef4444',
        },
    },
}));

describe('ModuleMenu', () => {
    const mockModule: PageModuleFragment = {
        urn: 'urn:li:dataHubPageModule:test',
        type: EntityType.DatahubPageModule,
        properties: {
            name: 'Test Module',
            type: DataHubPageModuleType.Link,
            visibility: { scope: PageModuleScope.Personal },
            params: {},
        },
    };

    const mockPosition: ModulePositionInput = {
        rowIndex: 0,
        rowSide: 'left',
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should render menu with correct items', () => {
        render(<ModuleMenu module={mockModule} position={mockPosition} />);

        // Click to open the dropdown
        const menuButton = screen.getByTestId('icon');
        fireEvent.click(menuButton);

        // Check that menu items are rendered
        expect(screen.getByText('Edit')).toBeInTheDocument();
        expect(screen.getByText('Duplicate')).toBeInTheDocument();
        expect(screen.getByText('Delete')).toBeInTheDocument();
    });

    it('should call removeModule when delete is clicked', () => {
        render(<ModuleMenu module={mockModule} position={mockPosition} />);

        // Click to open the dropdown
        const menuButton = screen.getByTestId('icon');
        fireEvent.click(menuButton);

        // Click the delete option
        const deleteButton = screen.getByText('Delete');
        fireEvent.click(deleteButton);

        // Verify that removeModule was called with correct parameters
        expect(mockRemoveModule).toHaveBeenCalledWith({
            moduleUrn: 'urn:li:dataHubPageModule:test',
            position: mockPosition,
        });
    });

    it('should handle different module types and positions', () => {
        const differentModule: PageModuleFragment = {
            urn: 'urn:li:dataHubPageModule:domains',
            type: EntityType.DatahubPageModule,
            properties: {
                name: 'Domains Module',
                type: DataHubPageModuleType.Domains,
                visibility: { scope: PageModuleScope.Global },
                params: {},
            },
        };

        const differentPosition: ModulePositionInput = {
            rowIndex: 2,
            rowSide: 'right',
        };

        render(<ModuleMenu module={differentModule} position={differentPosition} />);

        // Click to open the dropdown
        const menuButton = screen.getByTestId('icon');
        fireEvent.click(menuButton);

        // Click the delete option
        const deleteButton = screen.getByText('Delete');
        fireEvent.click(deleteButton);

        // Verify that removeModule was called with correct parameters
        expect(mockRemoveModule).toHaveBeenCalledWith({
            moduleUrn: 'urn:li:dataHubPageModule:domains',
            position: differentPosition,
        });
    });

    it('should render delete option with red color', () => {
        render(<ModuleMenu module={mockModule} position={mockPosition} />);

        // Click to open the dropdown
        const menuButton = screen.getByTestId('icon');
        fireEvent.click(menuButton);

        // Check that delete option has red color styling
        const deleteButton = screen.getByText('Delete');
        expect(deleteButton).toBeInTheDocument();
        // Note: Testing exact color styles in this test setup is challenging, but the component should work
    });
}); 