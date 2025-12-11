/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
