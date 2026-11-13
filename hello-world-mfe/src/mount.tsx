import React from 'react';
import { createRoot, Root } from 'react-dom/client';
import { App } from './App';

/**
 * Mount function that DataHub MFE framework calls to render this micro-frontend.
 * 
 * @param container - The DOM element where the MFE should be rendered
 * @param _options - Options passed from the host (currently unused)
 * @returns A cleanup function that unmounts the React app
 */
export function mount(container: HTMLElement, _options: Record<string, unknown> = {}): () => void {
    console.log('[HelloWorld MFE] Mounting...');
    
    const root: Root = createRoot(container);
    root.render(
        <React.StrictMode>
            <App />
        </React.StrictMode>
    );

    console.log('[HelloWorld MFE] Mounted successfully!');

    // Return cleanup function
    return () => {
        console.log('[HelloWorld MFE] Unmounting...');
        root.unmount();
    };
}

// Also export as default for flexibility
export default mount;

