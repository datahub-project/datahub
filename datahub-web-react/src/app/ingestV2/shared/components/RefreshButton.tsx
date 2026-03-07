import { Button } from '@components';
import React from 'react';
import { ArrowClockwise } from '@phosphor-icons/react/dist/csr/ArrowClockwise';

interface Props {
 id?: string;
 onClick?: () => void;
}

export default function RefreshButton({ id, onClick }: Props) {
 return (
 <Button id={id} variant="text" onClick={onClick} icon={{ icon: ArrowClockwise}}>
 Refresh
 </Button>
 );
}
