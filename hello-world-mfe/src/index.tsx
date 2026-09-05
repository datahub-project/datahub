/**
 * Entry point for standalone development.
 * When running the MFE independently (not loaded by DataHub), 
 * this file mounts the app to the #root element.
 */
import { mount } from './mount';

const rootElement = document.getElementById('root');

if (rootElement) {
    // Mount the app for standalone development
    const cleanup = mount(rootElement, {});
    
    // Enable hot module replacement cleanup
    if (module.hot) {
        module.hot.accept('./mount', () => {
            cleanup();
            const { mount: newMount } = require('./mount');
            newMount(rootElement, {});
        });
    }
}

