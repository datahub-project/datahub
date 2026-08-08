// Zone.js must be imported before Angular
import 'zone.js';

import { platformBrowserDynamic } from '@angular/platform-browser-dynamic';
import { AppModule } from './app/app.module';

/**
 * Bootstrap the Angular application for standalone development.
 * When running via `ng serve`, this file is the entry point.
 */
platformBrowserDynamic()
    .bootstrapModule(AppModule)
    .catch((err) => console.error(err));
