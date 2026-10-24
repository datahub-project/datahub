// Zone.js must be imported before Angular
import 'zone.js';

import { NgModuleRef, ApplicationRef, NgZone } from '@angular/core';
import { platformBrowserDynamic } from '@angular/platform-browser-dynamic';
import { AppModule } from './app/app.module';

/**
 * Mount function that DataHub MFE framework calls to render this Angular micro-frontend.
 *
 * This function bootstraps the Angular application into the provided container element.
 * It returns a cleanup function that properly destroys the Angular module.
 *
 * @param container - The DOM element where the MFE should be rendered
 * @param _options - Options passed from the host (currently unused)
 * @returns A cleanup function that destroys the Angular application
 */
export function mount(
    container: HTMLElement,
    _options: Record<string, unknown> = {}
): () => void {
    console.log('[Angular MFE] Mounting...');

    // Create a root element for Angular to bootstrap into
    const appRoot = document.createElement('app-root');
    container.appendChild(appRoot);

    let moduleRef: NgModuleRef<AppModule> | null = null;

    // Bootstrap Angular application
    platformBrowserDynamic()
        .bootstrapModule(AppModule, {
            ngZone: new NgZone({ enableLongStackTrace: false }),
        })
        .then((ref) => {
            moduleRef = ref;
            console.log('[Angular MFE] Mounted successfully!');
        })
        .catch((err) => {
            console.error('[Angular MFE] Bootstrap failed:', err);
        });

    // Return cleanup function
    return () => {
        console.log('[Angular MFE] Unmounting...');

        if (moduleRef) {
            // Destroy the Angular module
            moduleRef.destroy();
        }

        // Remove the app-root element
        if (appRoot.parentNode) {
            appRoot.parentNode.removeChild(appRoot);
        }

        console.log('[Angular MFE] Unmounted successfully!');
    };
}

// Also export as default for flexibility
export default mount;
