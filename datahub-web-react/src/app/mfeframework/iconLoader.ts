import { AppWindow } from '@phosphor-icons/react/dist/csr/AppWindow';
import type { ComponentType } from 'react';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyComponent = ComponentType<any>;

// Glob lives here (not in lazyIconRegistry.tsx) so the 1500-entry filename map
// is in this lazy chunk, never in the main bundle.
const iconModules = import.meta.glob('./lazy-icons/*.ts');

export async function loadIcon(name: string): Promise<{ default: AnyComponent }> {
    const loader = iconModules[`./lazy-icons/${name}.ts`];
    if (!loader) {
        console.warn(`[LazyIcon] Unknown icon "${name}", falling back to AppWindow`);
        return { default: AppWindow as AnyComponent };
    }
    const mod = (await loader()) as Record<string, AnyComponent>;
    return { default: mod[name] ?? (AppWindow as AnyComponent) };
}
