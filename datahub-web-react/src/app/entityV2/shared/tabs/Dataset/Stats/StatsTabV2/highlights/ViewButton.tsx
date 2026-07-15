import { Button } from '@components';
import { ArrowDown } from '@phosphor-icons/react/dist/csr/ArrowDown';
import React from 'react';
import { useTranslation } from 'react-i18next';

export const ViewButton = () => {
    const { t: tc } = useTranslation('common.actions');
    return (
        <Button variant="text" icon={{ icon: ArrowDown }} data-testid="view-button">
            {tc('view')}
        </Button>
    );
};
