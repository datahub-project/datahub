import React, { useState } from 'react';

import {
    PanelContainer,
    PanelContent,
    PanelHeader,
    ToggleIcon,
} from '@components/components/CollapsiblePanel/components';
import { CollapsiblePanelProps } from '@components/components/CollapsiblePanel/types';

/**
 * A collapsible panel component with smooth toggle animation.
 * Useful for organizing content in expandable sections.
 */
export function CollapsiblePanel({ header, children, defaultOpen = false, dataTestId }: CollapsiblePanelProps) {
    const [isOpen, setIsOpen] = useState(defaultOpen);

    return (
        <PanelContainer data-testid={dataTestId}>
            <PanelHeader
                onClick={() => setIsOpen(!isOpen)}
                data-testid={dataTestId ? `${dataTestId}-header` : undefined}
            >
                <ToggleIcon $isOpen={isOpen} size={16} weight="bold" />
                {header}
            </PanelHeader>
            <PanelContent $isOpen={isOpen}>{children}</PanelContent>
        </PanelContainer>
    );
}
