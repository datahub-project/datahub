import { AppWindow } from '@phosphor-icons/react/dist/csr/AppWindow';
import React, { Suspense } from 'react';

// Resolves Phosphor icons by name for admin-configured features (e.g. MFE nav, custom pages).
// Two-level lazy split keeps the main bundle icon-free:
//   1. iconLoader.ts (holds the glob map) is loaded once on first icon request
//   2. Each icon stub in lazy-icons/ is its own async chunk — only requested icons download
const loadIconLoader = () => import('./iconLoader');

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyComponent = React.ComponentType<any>;

const iconCache = new Map<string, React.LazyExoticComponent<AnyComponent>>();

function getCachedLazyIcon(name: string): React.LazyExoticComponent<AnyComponent> {
    if (!iconCache.has(name)) {
        iconCache.set(
            name,
            React.lazy(async (): Promise<{ default: AnyComponent }> => {
                const { loadIcon } = await loadIconLoader();
                return loadIcon(name);
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
