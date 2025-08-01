import React from 'react';

import { ViewSelect } from '@app/entityV2/view/select/ViewSelect';
import ViewSelectContextProvider from '@app/entityV2/view/select/ViewSelectContext';

interface Props {
    isOpen?: boolean;
    onOpenChange?: (isOpen: boolean) => void;
}

export default function ViewSelectButtonWithPopover({ isOpen, onOpenChange }: Props) {
    return (
        <ViewSelectContextProvider isOpen={isOpen} onOpenChange={onOpenChange}>
            <ViewSelect />
        </ViewSelectContextProvider>
    );
}
