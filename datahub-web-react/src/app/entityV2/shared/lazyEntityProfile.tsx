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

type BoundaryProps = {
    children?: React.ReactNode;
};

type BoundaryState = {
    failed: boolean;
};

/**
 * A rejected import() is a render error. Suspense does not catch it, and React.lazy keeps the
 * rejected promise, so the fallback reloads the page instead of resetting this boundary.
 */
class ProfileChunkBoundary extends React.Component<BoundaryProps, BoundaryState> {
    constructor(props: BoundaryProps) {
        super(props);
        this.state = { failed: false };
    }

    static getDerivedStateFromError(): BoundaryState {
        return { failed: true };
    }

    render() {
        if (this.state.failed) {
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

/**
 * Profile tabs, sidebars, and embedded profiles are rendered through this wrapper so their
 * modules stay out of the logged-in shell. Search and home only need icons and preview cards.
 * The import() call site must stay next to a literal path; Rollup will not split a variable path
 * hidden inside this helper.
 *
 * Props stay loose so the wrapper assigns to the tab and sidebar component slots. Sidebars key
 * sections by displayName, so each wrapper sets that to the component name.
 */
export function lazyProfileComponent(
    displayName: string,
    loader: () => Promise<{ default: React.ComponentType<any> }>,
): React.FunctionComponent<any> {
    const LazyComponent = React.lazy(loader);

    function ProfileComponent(props: any) {
        return (
            <ProfileChunkBoundary>
                <Suspense
                    fallback={
                        <ProfileFallback>
                            <Loader />
                        </ProfileFallback>
                    }
                >
                    <LazyComponent {...props} />
                </Suspense>
            </ProfileChunkBoundary>
        );
    }

    ProfileComponent.displayName = displayName;
    return ProfileComponent;
}
