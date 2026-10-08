import type { IconProps } from '@phosphor-icons/react';
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

type LazyIconProps = IconProps;

function IconFallback({ size }: { size?: string | number }): JSX.Element {
    const dimension = typeof size === 'number' ? size : Number(size) || 16;
    return (
        <span
            aria-hidden
            style={{
                display: 'inline-block',
                width: dimension,
                height: dimension,
            }}
        />
    );
}

export function getLazyIcon(name: string, props?: LazyIconProps): JSX.Element {
    const LazyIcon = getCachedLazyIcon(name);
    return (
        <Suspense fallback={<IconFallback size={props?.size} />}>
            <LazyIcon {...props} />
        </Suspense>
    );
}
