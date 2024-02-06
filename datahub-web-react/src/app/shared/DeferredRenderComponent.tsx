import React, { useEffect, useState } from 'react';

interface Props {
    wrappedComponent: React.ReactNode;
    loadingComponent?: React.ReactNode;
    delay?: number;
}

export function DeferredRenderComponent({ wrappedComponent, loadingComponent, delay = 250 }: Props) {
    const [shouldRender, setShouldRender] = useState(false);

    useEffect(() => {
        setTimeout(() => {
            setShouldRender(true);
        }, delay);
    }, [delay]);

    if (shouldRender) {
        return <>{wrappedComponent}</>;
    }

    return loadingComponent ? <>{loadingComponent}</> : null;
}
