import { Button, Tooltip } from '@components';
import { message } from 'antd';
import React, { useCallback, useState } from 'react';
import styled from 'styled-components';

import { EntitySearchDropdown } from '@app/entityV2/shared/EntitySearchSelect/EntitySearchDropdown';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { AndFilterInput, EntityType } from '@types';

const ActionsContainer = styled.div`
    display: flex;
    gap: 8px;
    padding: 0px;
    justify-content: flex-end;
`;

const AddButton = styled(Button)`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 24px;
    height: 24px;
    padding: 0;
    margin: 0;
    border: none;
    border-radius: 4px;
    background: transparent;
    color: ${colors.gray[400]};
    cursor: pointer;
    transition: all 0.2s ease;

    svg {
        width: 20px;
        height: 20px;
    }
`;

export interface AddRelatedEntityDropdownProps {
    entityTypes: EntityType[];
    existingUrns: Set<string>;
    documentUrn: string;
    onConfirm: (selectedUrns: string[]) => Promise<void>;
    placeholder?: string;
    defaultFilters?: AndFilterInput[];
    viewUrn?: string;
    disabled?: boolean;
    // Initial selected URNs (existing related entities) - these will be pre-selected in the dropdown
    initialSelectedUrns?: string[];
}

/**
 * A dropdown component for adding related entities to a document.
 * Encapsulates the search, selection, and confirmation logic.
 */
export const AddRelatedEntityDropdown: React.FC<AddRelatedEntityDropdownProps> = ({
    entityTypes,
    existingUrns: _existingUrns,
    documentUrn,
    onConfirm,
    placeholder = 'Add related entities...',
    defaultFilters,
    viewUrn,
    disabled = false,
    initialSelectedUrns = [],
}) => {
    const [isDropdownOpen, setIsDropdownOpen] = useState(false);
    const [selectedUrns, setSelectedUrns] = useState<string[]>(initialSelectedUrns);

    const handleConfirmAdd = useCallback(async () => {
        // Filter out the document itself (can't relate a document to itself)
        const finalUrns = selectedUrns.filter((urn) => urn !== documentUrn);

        try {
            // Pass the final list of URNs (includes both additions and removals)
            // The parent will handle replacing the entire list
            await onConfirm(finalUrns);
            setIsDropdownOpen(false);
            // Reset to initial state when closing
            setSelectedUrns(initialSelectedUrns);
        } catch (error) {
            message.error('Failed to update related entities. An unexpected error occurred!');
            console.error('Failed to update related entities:', error);
        }
    }, [selectedUrns, documentUrn, onConfirm, initialSelectedUrns]);

    const handleCancel = useCallback(() => {
        setIsDropdownOpen(false);
        // Reset to initial state when canceling
        setSelectedUrns(initialSelectedUrns);
    }, [initialSelectedUrns]);

    const actionButtons = (
        <ActionsContainer>
            <Button onClick={handleCancel} variant="secondary" size="sm">
                Cancel
            </Button>
            <Button onClick={handleConfirmAdd} size="sm" disabled={selectedUrns.length === 0}>
                Update
            </Button>
        </ActionsContainer>
    );

    const triggerButton = (
        <Tooltip title="Link related assets or context docs" placement="bottom">
            <AddButton
                variant="text"
                isCircle
                icon={{ icon: 'Plus', source: 'phosphor' }}
                aria-label="Add related entity"
                disabled={disabled}
            />
        </Tooltip>
    );

    return (
        <Tooltip title="Add related entity" placement="bottom">
            <EntitySearchDropdown
                entityTypes={entityTypes}
                selectedUrns={selectedUrns}
                onSelectionChange={setSelectedUrns}
                placeholder={placeholder}
                defaultFilters={defaultFilters}
                viewUrn={viewUrn}
                trigger={triggerButton}
                open={isDropdownOpen}
                onOpenChange={(open) => {
                    setIsDropdownOpen(open);
                    if (!open) {
                        // Reset to initial state when closing without confirming
                        setSelectedUrns(initialSelectedUrns);
                    } else {
                        // Initialize with existing URNs when opening
                        setSelectedUrns(initialSelectedUrns);
                    }
                }}
                placement="top"
                disabled={disabled}
                actionButtons={actionButtons}
                dropdownContainerStyle={{
                    minWidth: '400px',
                    maxHeight: '500px',
                }}
            />
        </Tooltip>
    );
};
