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
