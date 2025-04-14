import { OwnerEntityType } from '../../../types.generated';

export type CreateNewTagModalProps = {
    open: boolean;
    onClose: () => void;
};

// Interface for modal buttons matching the expected ButtonProps
export interface ModalButton {
    text: string;
    color: 'violet' | 'white' | 'black' | 'green' | 'red' | 'blue' | 'yellow' | 'gray';
    variant: 'text' | 'filled' | 'outline';
    onClick: () => void;
    id?: string;
    disabled?: boolean;
    isLoading?: boolean;
}

// Interface for pending owner
export interface PendingOwner {
    ownerUrn: string;
    ownerEntityType: OwnerEntityType;
    ownershipTypeUrn: string;
}

// Common styled components
export const FormSection = {
    marginBottom: '16px',
};

// Owners section props
export interface OwnersSectionProps {
    selectedOwnerUrns: string[];
    setSelectedOwnerUrns: React.Dispatch<React.SetStateAction<string[]>>;
    pendingOwners: PendingOwner[];
    setPendingOwners: React.Dispatch<React.SetStateAction<PendingOwner[]>>;
}

// Entities section props
export interface EntitiesSectionProps {
    selectedEntityUrns: string[];
    setSelectedEntityUrns: React.Dispatch<React.SetStateAction<string[]>>;
}

// Create tag result interface
export interface CreateTagResult {
    tagUrn: string;
    success: boolean;
    error?: Error;
}
