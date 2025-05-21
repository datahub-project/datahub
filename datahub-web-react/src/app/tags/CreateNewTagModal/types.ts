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

// Common styled components
export const FormSection = {
    marginBottom: '16px',
};

// Create tag result interface
export interface CreateTagResult {
    tagUrn: string;
    success: boolean;
    error?: Error;
}
