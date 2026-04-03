import { Text } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { FolderOpen } from '@phosphor-icons/react/dist/csr/FolderOpen';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React, { useState } from 'react';
import styled from 'styled-components';

import { DocumentTreeNode } from '@app/document/DocumentTreeContext';

const SelectorContainer = styled.div`
    border: 1px solid ${({ theme }) => theme.colors.border};
    border-radius: 8px;
    overflow: hidden;
`;

const SelectorHeader = styled.button`
    display: flex;
    align-items: center;
    gap: 8px;
    width: 100%;
    padding: 10px 12px;
    background: transparent;
    border: none;
    cursor: pointer;
    transition: background-color 0.15s ease;
    color: ${({ theme }) => theme.colors.textSecondary};

    &:hover {
        background-color: ${({ theme }) => theme.colors.bgHover};
    }
`;

const HeaderContent = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    flex: 1;
    min-width: 0;
`;

const ParentLabel = styled.span`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const ClearButton = styled.button`
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 4px;
    border: none;
    background: transparent;
    cursor: pointer;
    border-radius: 4px;
    color: ${({ theme }) => theme.colors.textSecondary};

    &:hover {
        background-color: ${({ theme }) => theme.colors.bgHover};
        color: ${({ theme }) => theme.colors.text};
    }
`;

const CaretIcon = styled(CaretDown)<{ $isOpen: boolean }>`
    transition: transform 0.15s ease;
    transform: rotate(${({ $isOpen }) => ($isOpen ? '180deg' : '0deg')});
    flex-shrink: 0;
    color: ${({ theme }) => theme.colors.textSecondary};
`;

const TreePanel = styled.div`
    max-height: 240px;
    overflow-y: auto;
    border-top: 1px solid ${({ theme }) => theme.colors.border};
    padding: 4px;

    &::-webkit-scrollbar {
        width: 6px;
    }

    &::-webkit-scrollbar-track {
        background: transparent;
    }

    &::-webkit-scrollbar-thumb {
        background: ${({ theme }) => theme.colors.scrollbarThumb};
        border-radius: 3px;
    }
`;

const RootOption = styled.div<{ $isSelected: boolean }>`
    padding: 8px 12px;
    border-radius: 6px;
    cursor: pointer;
    font-size: 14px;
    transition: background-color 0.15s ease;

    ${({ $isSelected, theme }) =>
        $isSelected
            ? `background: ${theme.colors.bgSelected};`
            : `&:hover { background-color: ${theme.colors.bgHover}; }`}
`;

const StyledFolderIcon = styled(FolderOpen)`
    color: ${({ theme }) => theme.colors.textSecondary};
    flex-shrink: 0;
`;

interface ImportParentSelectorProps {
    selectedParentUrn: string | null;
    onSelectParent: (urn: string | null) => void;
    getNode?: (urn: string) => DocumentTreeNode | undefined;
}

export default function ImportParentSelector({
    selectedParentUrn,
    onSelectParent,
    getNode,
}: ImportParentSelectorProps) {
    const [isOpen, setIsOpen] = useState(false);

    const selectedNode = selectedParentUrn && getNode ? getNode(selectedParentUrn) : null;
    const displayName = selectedNode?.title || (selectedParentUrn ? 'Selected document' : null);

    return (
        <SelectorContainer>
            <SelectorHeader type="button" onClick={() => setIsOpen(!isOpen)}>
                <StyledFolderIcon size={18} />
                <HeaderContent>
                    <Text size="sm" color="gray" colorLevel={600}>
                        Import into:
                    </Text>
                    <ParentLabel>
                        <Text size="sm" weight="semiBold">
                            {displayName || 'Root (top level)'}
                        </Text>
                    </ParentLabel>
                </HeaderContent>
                {selectedParentUrn && (
                    <ClearButton
                        type="button"
                        onClick={(e) => {
                            e.stopPropagation();
                            onSelectParent(null);
                        }}
                        title="Clear parent — import at root"
                    >
                        <X size={14} />
                    </ClearButton>
                )}
                <CaretIcon size={14} $isOpen={isOpen} />
            </SelectorHeader>

            {isOpen && (
                <TreePanel>
                    <RootOption
                        $isSelected={selectedParentUrn === null}
                        onClick={() => {
                            onSelectParent(null);
                            setIsOpen(false);
                        }}
                    >
                        <Text size="sm">Root (top level)</Text>
                    </RootOption>
                </TreePanel>
            )}
        </SelectorContainer>
    );
}
