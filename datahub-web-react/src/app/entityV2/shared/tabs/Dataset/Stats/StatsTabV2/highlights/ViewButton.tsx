import { Button } from '@components';
import React from 'react';

export const ViewButton = () => {
    return (
        <Button variant="text" icon={{ icon: 'ArrowDown' }} data-testid="view-button">
            View
        </Button>
    );
};
