import { Button } from '@components';
import React from 'react';

interface Props {
    id?: string;
    onClick?: () => void;
}

export default function RefreshButton({ id, onClick }: Props) {
    return (
        <Button id={id} variant="text" onClick={onClick} icon={{ icon: 'ArrowClockwise', source: 'phosphor' }}>
            Refresh
        </Button>
    );
}
