import React from 'react';

export interface CollapsiblePanelProps {
    /** Content to display in the panel header */
    header: React.ReactNode;
    /** Content to display in the collapsible panel body */
    children: React.ReactNode;
    /** Whether the panel should be open by default */
    defaultOpen?: boolean;
    /** Optional test ID for testing */
    dataTestId?: string;
}
