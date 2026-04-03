import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import ImportParentSelector from '@app/context/import/ImportParentSelector';
import '@app/context/import/__tests__/testSetup';
import CustomThemeProvider from '@src/CustomThemeProvider';

const Wrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
    <CustomThemeProvider>{children}</CustomThemeProvider>
);

describe('ImportParentSelector', () => {
    it('shows "Root (top level)" when no parent is selected', () => {
        render(
            <Wrapper>
                <ImportParentSelector selectedParentUrn={null} onSelectParent={vi.fn()} />
            </Wrapper>,
        );

        expect(screen.getByText('Root (top level)')).toBeInTheDocument();
        expect(screen.getByText('Import into:')).toBeInTheDocument();
    });

    it('shows node title when parent is selected and getNode returns data', () => {
        const getNode = vi.fn().mockReturnValue({ title: 'My Folder', urn: 'urn:li:document:folder1' });

        render(
            <Wrapper>
                <ImportParentSelector
                    selectedParentUrn="urn:li:document:folder1"
                    onSelectParent={vi.fn()}
                    getNode={getNode}
                />
            </Wrapper>,
        );

        expect(screen.getByText('My Folder')).toBeInTheDocument();
        expect(getNode).toHaveBeenCalledWith('urn:li:document:folder1');
    });

    it('shows "Selected document" fallback when getNode returns undefined', () => {
        const getNode = vi.fn().mockReturnValue(undefined);

        render(
            <Wrapper>
                <ImportParentSelector
                    selectedParentUrn="urn:li:document:unknown"
                    onSelectParent={vi.fn()}
                    getNode={getNode}
                />
            </Wrapper>,
        );

        expect(screen.getByText('Selected document')).toBeInTheDocument();
    });

    it('shows clear button when a parent is selected', () => {
        render(
            <Wrapper>
                <ImportParentSelector
                    selectedParentUrn="urn:li:document:folder1"
                    onSelectParent={vi.fn()}
                    getNode={vi.fn().mockReturnValue({ title: 'Folder' })}
                />
            </Wrapper>,
        );

        const clearButton = screen.getByTitle('Clear parent — import at root');
        expect(clearButton).toBeInTheDocument();
    });

    it('calls onSelectParent with null when clear is clicked', () => {
        const onSelectParent = vi.fn();

        render(
            <Wrapper>
                <ImportParentSelector
                    selectedParentUrn="urn:li:document:folder1"
                    onSelectParent={onSelectParent}
                    getNode={vi.fn().mockReturnValue({ title: 'Folder' })}
                />
            </Wrapper>,
        );

        fireEvent.click(screen.getByTitle('Clear parent — import at root'));
        expect(onSelectParent).toHaveBeenCalledWith(null);
    });

    it('does not show clear button when no parent is selected', () => {
        render(
            <Wrapper>
                <ImportParentSelector selectedParentUrn={null} onSelectParent={vi.fn()} />
            </Wrapper>,
        );

        expect(screen.queryByTitle('Clear parent — import at root')).not.toBeInTheDocument();
    });
});
