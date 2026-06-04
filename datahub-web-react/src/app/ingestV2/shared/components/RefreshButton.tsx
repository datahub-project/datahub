import { Button } from '@components';
import { ArrowClockwise } from '@phosphor-icons/react/dist/csr/ArrowClockwise';
import React from 'react';
import { useTranslation } from 'react-i18next';

interface Props {
    id?: string;
    onClick?: () => void;
}

export default function RefreshButton({ id, onClick }: Props) {
    const { t: tc } = useTranslation('common.actions');
    return (
        <Button id={id} variant="text" onClick={onClick} icon={{ icon: ArrowClockwise }}>
            {tc('refresh')}
        </Button>
    );
}
