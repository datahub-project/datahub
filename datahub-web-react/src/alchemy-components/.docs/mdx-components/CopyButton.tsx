import React from 'react';

import { Button, Icon } from '@components';
import { copyToClipboard } from './utils';

interface Props {
    text: string;
}

export const CopyButton = ({ text }: Props) => (
    <div style={{ display: 'inline-block' }}>
        <Button variant="text" color="gray" size="sm" onClick={() => copyToClipboard(text)}>
            <Icon icon="ContentCopy" size="xs" />
        </Button>
    </div>
);
