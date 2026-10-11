import { AppWindow } from '@phosphor-icons/react/dist/csr/AppWindow';
import type { ComponentType } from 'react';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyComponent = ComponentType<any>;

type GlobMap = Record<string, () => Promise<unknown>>;

// Exported for testing — accepts an injected glob map so tests don't need import.meta.glob.
export function makeLoadIcon(iconModules: GlobMap) {
    return async function loadIcon(name: string): Promise<{ default: AnyComponent }> {
        const loader = iconModules[`./lazy-icons/${name}.ts`];
        if (!loader) {
            console.warn(`[LazyIcon] Unknown icon "${name}", falling back to AppWindow`);
            return { default: AppWindow as AnyComponent };
        }
        const mod = (await loader()) as Record<string, AnyComponent>;
        // Guard: if the named export is absent (e.g. stripped by a build tool), fall back
        // rather than passing undefined to React as a component.
        return { default: mod[name] ?? (AppWindow as AnyComponent) };
    };
}

// Glob lives in this lazy chunk (not in lazyIconRegistry.tsx / main bundle).
export const loadIcon = makeLoadIcon(import.meta.glob('./lazy-icons/*.ts') as GlobMap);
