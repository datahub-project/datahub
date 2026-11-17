import { Button, Loader, Text } from '@components';
import React, { Suspense } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const ProfileFallback = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    width: 100%;
    min-height: 48px;
    gap: 8px;
    padding: 12px;
`;

/** Errors from rejected import()s, so the boundary can tell them apart from render errors. */
const chunkLoadErrors = new WeakSet<Error>();

function markChunkLoadError(error: unknown): Error {
    const chunkError = error instanceof Error ? error : new Error(String(error));
    chunkLoadErrors.add(chunkError);
    return chunkError;
}

type BoundaryProps = {
    children?: React.ReactNode;
    propagateRenderErrors: boolean;
};

type BoundaryState = {
    error?: Error;
};

/**
 * A rejected import() is a render error. Suspense does not catch it, and React.lazy keeps the
 * rejected promise, so the fallback reloads the page instead of resetting this boundary.
 * With `propagateRenderErrors`, any other error is rethrown to the nearest parent boundary.
 */
class ProfileChunkBoundary extends React.Component<BoundaryProps, BoundaryState> {
    constructor(props: BoundaryProps) {
        super(props);
        this.state = {};
    }

    static getDerivedStateFromError(error: unknown): BoundaryState {
        return { error: error instanceof Error ? error : new Error(String(error)) };
    }

    render() {
        const { error } = this.state;
        if (error) {
            if (this.props.propagateRenderErrors && !chunkLoadErrors.has(error)) {
                throw error;
            }
            return <ProfileChunkFallback />;
        }
        return this.props.children;
    }
}

function ProfileChunkFallback() {
    const { t } = useTranslation('shared.error');
    const { t: actions } = useTranslation('common.actions');
    return (
        <ProfileFallback>
            <Text size="sm">{t('fallback.title')}</Text>
            <Button variant="outline" size="sm" onClick={() => window.location.reload()}>
                {actions('refresh')}
            </Button>
        </ProfileFallback>
    );
}

type ChunkModule<C> = { default: C };

/** The loaded component, plus `preload` to start its import ahead of the first render. */
export type LazyProfileComponent<C extends React.FunctionComponent<any>> = C & {
    preload: () => Promise<ChunkModule<C>>;
};

export type LazyProfileOptions<P> = {
    /** Rendered while the chunk loads. Defaults to a small centered spinner. */
    fallback?: (props: P) => React.ReactElement;
    /**
     * Rethrow errors thrown while rendering the loaded component, so a parent boundary with
     * its own retry handles them. Failed imports still show the reload prompt.
     */
    propagateRenderErrors?: boolean;
};

/**
 * Profile tabs, sidebars, and embedded profiles are rendered through this wrapper so their
 * modules stay out of the logged-in shell. Search and home only need icons and preview cards.
 * The import() call site must stay next to a literal path; Rollup will not split a variable path
 * hidden inside this helper.
 *
 * The wrapper is typed as the component it loads, so callers keep its real props and generics
 * (e.g. inline `tabs` passed to `EntityProfile` are checked against `EntityTab`). Loaded components
 * must be plain function components: the wrapper forwards no refs and copies no statics. Sidebars
 * key sections by displayName, so each wrapper sets that to the component name.
 *
 * `preload` starts that same import. The wrapper always renders the lazy component, so a
 * later parent render keeps the mounted section instead of swapping in another element type.
 */
export function lazyProfileComponent<C extends React.FunctionComponent<any>>(
    displayName: string,
    loader: () => Promise<ChunkModule<C>>,
    options: LazyProfileOptions<React.ComponentProps<C>> = {},
): LazyProfileComponent<C> {
    let pending: Promise<ChunkModule<C>> | undefined;

    function load() {
        pending ??= loader().catch((error: unknown) => {
            throw markChunkLoadError(error);
        });
        return pending;
    }

    const LazyComponent = React.lazy(load);

    function ProfileComponent(props: any) {
        return (
            <ProfileChunkBoundary propagateRenderErrors={!!options.propagateRenderErrors}>
                <Suspense
                    fallback={
                        options.fallback ? (
                            options.fallback(props)
                        ) : (
                            <ProfileFallback>
                                <Loader />
                            </ProfileFallback>
                        )
                    }
                >
                    <LazyComponent {...props} />
                </Suspense>
            </ProfileChunkBoundary>
        );
    }

    // Props are forwarded unchanged, so the wrapper can stand in for the loaded component.
    return Object.assign(ProfileComponent, { displayName, preload: load }) as unknown as LazyProfileComponent<C>;
}
