import { Button } from '@components';
import { ArrowDown } from '@phosphor-icons/react/dist/csr/ArrowDown';
import React from 'react';

export const ViewButton = () => {
    return (
        <Button variant="text" icon={{ icon: ArrowDown }} data-testid="view-button">
            View
        </Button>
    );
};
