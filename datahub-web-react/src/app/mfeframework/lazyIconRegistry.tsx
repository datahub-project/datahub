import { AppWindow } from '@phosphor-icons/react/dist/csr/AppWindow';
import React, { Suspense } from 'react';

// Resolves Phosphor icons by name for admin-configured features (e.g. MFE nav, custom pages).
// Icons are never bundled upfront — each loads as a separate async chunk on demand, so end
// users only download the specific icons an admin chose, not all 1500+.
const iconModules = import.meta.glob('./lazy-icons/*.ts');

const iconCache = new Map<string, React.LazyExoticComponent<React.ComponentType>>();

function getCachedLazyIcon(name: string): React.LazyExoticComponent<React.ComponentType> {
    if (!iconCache.has(name)) {
        const loader = iconModules[`./lazy-icons/${name}.ts`];
        iconCache.set(
            name,
            React.lazy(async () => {
                if (!loader) {
                    console.warn(`[LazyIcon] Unknown icon "${name}", falling back to AppWindow`);
                    return { default: AppWindow };
                }
                const mod = (await loader()) as Record<string, React.ComponentType>;
                return { default: mod[name] ?? AppWindow };
            }),
        );
    }
    return iconCache.get(name)!;
}

export function getLazyIcon(name: string): JSX.Element {
    const LazyIcon = getCachedLazyIcon(name);
    return (
        <Suspense fallback={<AppWindow />}>
            <LazyIcon />
        </Suspense>
    );
}
