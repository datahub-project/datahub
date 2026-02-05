import React, { useEffect, useRef } from 'react';

interface ExternalLinksWrapperProps {
    className?: string;
}

export function ExternalLinksWrapper({ children, className }: React.PropsWithChildren<ExternalLinksWrapperProps>) {
    const containerRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        const container = containerRef.current;
        if (!container) return;

        const links = container.querySelectorAll<HTMLAnchorElement>('a[href]');

        links.forEach((link) => {
            if (link.target === '_blank' && link.rel.includes('noopener')) {
                return;
            }

            // eslint-disable-next-line no-param-reassign
            link.target = '_blank';
            if (!link.rel.includes('noopener')) {
                // eslint-disable-next-line no-param-reassign
                link.rel = link.rel ? `${link.rel} noopener noreferrer` : 'noopener noreferrer';
            }
        });
    }, [children]);

    return (
        <div ref={containerRef} className={className} data-testid="external-links-wrapper">
            {children}
        </div>
    );
}
