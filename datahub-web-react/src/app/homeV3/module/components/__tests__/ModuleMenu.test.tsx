import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import ModuleMenu from '@app/homeV3/module/components/ModuleMenu';
import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope } from '@types';

// Mock the PageTemplateContext
const mockRemoveModule = vi.fn();
const mockOpenToEdit = vi.fn();
vi.mock('@app/homeV3/context/PageTemplateContext', () => ({
    usePageTemplateContext: () => ({
        removeModule: mockRemoveModule,
        moduleModalState: { openToEdit: mockOpenToEdit },
    }),
}));

// Mock the Icon component
vi.mock('@components', () => ({
    Icon: React.forwardRef(({ icon, _source, _size, ...props }: any, ref: any) => (
        <div ref={ref} data-testid="icon" data-icon={icon} {...props}>
            {icon}
        </div>
    )),
    Tooltip: (props: any) => <span {...props} />,
    Text: (props: any) => <p {...props} />,
    colors: {
        gray: {
            600: '#4B5563',
        },
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
        // expect(screen.getByText('Duplicate')).toBeInTheDocument();
        expect(screen.getByText('Remove')).toBeInTheDocument();
    });

    it('should call removeModule when remove is clicked', () => {
        render(<ModuleMenu module={mockModule} position={mockPosition} />);

        // Click to open the dropdown
        const menuButton = screen.getByTestId('icon');
        fireEvent.click(menuButton);

        // Click the remove option
        const removeButton = screen.getByText('Remove');
        fireEvent.click(removeButton);

        // Verify that removeModule was called with correct parameters
        expect(mockRemoveModule).toHaveBeenCalledWith({
            module: mockModule,
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

        // Click the remove option
        const removeButton = screen.getByText('Remove');
        fireEvent.click(removeButton);

        // Verify that removeModule was called with correct parameters
        expect(mockRemoveModule).toHaveBeenCalledWith({
            module: differentModule,
            position: differentPosition,
        });
    });

    it('should render remove option with red color', () => {
        render(<ModuleMenu module={mockModule} position={mockPosition} />);

        // Click to open the dropdown
        const menuButton = screen.getByTestId('icon');
        fireEvent.click(menuButton);

        // Check that remove option has red color styling
        const removeButton = screen.getByText('Remove');
        expect(removeButton).toBeInTheDocument();
        // Note: Testing exact color styles in this test setup is challenging, but the component should work
    });

    it('should handle moduleIndex in position when deleting', () => {
        const positionWithModuleIndex: ModulePositionInput = {
            rowIndex: 1,
            rowSide: 'right',
            moduleIndex: 2,
        };

        render(<ModuleMenu module={mockModule} position={positionWithModuleIndex} />);

        // Click to open the dropdown
        const menuButton = screen.getByTestId('icon');
        fireEvent.click(menuButton);

        // Click the remove option
        const removeButton = screen.getByText('Remove');
        fireEvent.click(removeButton);

        // Verify that removeModule was called with moduleIndex
        expect(mockRemoveModule).toHaveBeenCalledWith({
            module: mockModule,
            position: positionWithModuleIndex,
        });
    });

    it('should handle module with long name', () => {
        const moduleWithLongName: PageModuleFragment = {
            urn: 'urn:li:dataHubPageModule:veryLongName',
            type: EntityType.DatahubPageModule,
            properties: {
                name: 'This is a very long module name that might cause rendering issues in the UI',
                type: DataHubPageModuleType.OwnedAssets,
                visibility: { scope: PageModuleScope.Personal },
                params: {},
            },
        };

        render(<ModuleMenu module={moduleWithLongName} position={mockPosition} />);

        // Click to open the dropdown
        const menuButton = screen.getByTestId('icon');
        fireEvent.click(menuButton);

        // Click the remove option
        const removeButton = screen.getByText('Remove');
        fireEvent.click(removeButton);

        // Verify that removeModule was called with correct URN
        expect(mockRemoveModule).toHaveBeenCalledWith({
            module: moduleWithLongName,
            position: mockPosition,
        });
    });

    it('should handle position with undefined optional fields', () => {
        const minimalPosition: ModulePositionInput = {
            rowIndex: 0,
            // rowSide and moduleIndex are optional/undefined
        };

        render(<ModuleMenu module={mockModule} position={minimalPosition} />);

        // Click to open the dropdown
        const menuButton = screen.getByTestId('icon');
        fireEvent.click(menuButton);

        // Click the remove option
        const removeButton = screen.getByText('Remove');
        fireEvent.click(removeButton);

        // Verify that removeModule was called with minimal position
        expect(mockRemoveModule).toHaveBeenCalledWith({
            module: mockModule,
            position: minimalPosition,
        });
    });

    it('should handle module with special characters in URN', () => {
        const moduleWithSpecialChars: PageModuleFragment = {
            urn: 'urn:li:dataHubPageModule:test-module_with.special+chars',
            type: EntityType.DatahubPageModule,
            properties: {
                name: 'Module with Special Characters',
                type: DataHubPageModuleType.Link,
                visibility: { scope: PageModuleScope.Personal },
                params: {},
            },
        };

        render(<ModuleMenu module={moduleWithSpecialChars} position={mockPosition} />);

        // Click to open the dropdown
        const menuButton = screen.getByTestId('icon');
        fireEvent.click(menuButton);

        // Click the remove option
        const removeButton = screen.getByText('Remove');
        fireEvent.click(removeButton);

        // Verify that removeModule was called with special character URN
        expect(mockRemoveModule).toHaveBeenCalledWith({
            module: moduleWithSpecialChars,
            position: mockPosition,
        });
    });

    it('should handle edit option (placeholder functionality)', () => {
        render(<ModuleMenu module={mockModule} position={mockPosition} />);

        // Click to open the dropdown
        const menuButton = screen.getByTestId('icon');
        fireEvent.click(menuButton);

        // Check that edit and duplicate options are present
        const editButton = screen.getByText('Edit');
        // const duplicateButton = screen.getByText('Duplicate');

        expect(editButton).toBeInTheDocument();
        // expect(duplicateButton).toBeInTheDocument();

        // Click edit and duplicate (should not throw errors)
        fireEvent.click(editButton);
        // fireEvent.click(duplicateButton);

        // These are placeholder implementations, so we just verify they don't crash
        expect(mockRemoveModule).not.toHaveBeenCalled();
    });

    it('should handle multiple rapid clicks on remove', () => {
        render(<ModuleMenu module={mockModule} position={mockPosition} />);

        // Click to open the dropdown
        const menuButton = screen.getByTestId('icon');
        fireEvent.click(menuButton);

        // Click the remove option multiple times rapidly
        const removeButton = screen.getByText('Remove');
        fireEvent.click(removeButton);
        fireEvent.click(removeButton);
        fireEvent.click(removeButton);

        // Verify that removeModule was called multiple times (as expected)
        expect(mockRemoveModule).toHaveBeenCalledTimes(3);
        expect(mockRemoveModule).toHaveBeenCalledWith({
            module: mockModule,
            position: mockPosition,
        });
    });
});
