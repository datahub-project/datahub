import { Loader } from '@components';
import React, { Suspense } from 'react';
import styled from 'styled-components';

const ProfileFallback = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 100%;
    min-height: 48px;
`;

/**
 * Profile tabs, sidebars, and embedded profiles are rendered through this wrapper so their
 * modules stay out of the logged-in shell. Search and home only need icons and preview cards.
 * The import() call site must stay in the entity module with a literal path; Rollup will not
 * split a path that is hidden inside this helper.
 */
export function lazyProfileComponent<P extends object>(
    displayName: string,
    loader: () => Promise<{ default: React.ComponentType<P> }>,
): React.ComponentType<P> {
    const LazyComponent = React.lazy(loader);

    function ProfileComponent(props: P) {
        return (
            <Suspense
                fallback={
                    <ProfileFallback>
                        <Loader />
                    </ProfileFallback>
                }
            >
                <LazyComponent {...props} />
            </Suspense>
        );
    }

    ProfileComponent.displayName = displayName;
    return ProfileComponent;
}
