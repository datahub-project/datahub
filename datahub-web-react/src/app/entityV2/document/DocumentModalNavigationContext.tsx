import { createContext, useContext } from 'react';

// Context for handling breadcrumb navigation within the document modal
export const DocumentModalNavigationContext = createContext<{
    navigateToDocument: ((urn: string) => void) | null;
}>({
    navigateToDocument: null,
});

export const useDocumentModalNavigation = () => {
    const context = useContext(DocumentModalNavigationContext);
    return context;
};
