/* eslint-disable rulesdir/no-hardcoded-colors */
import { Button, Icon } from '@components';
import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import React from 'react';

import { copyToClipboard } from './utils';

interface Props {
    text: string;
}

const CopyButton = ({ text }: Props) => (
    <div style={{ display: 'inline-block' }}>
        <Button variant="text" color="gray" size="sm" onClick={() => copyToClipboard(text)}>
            <Icon icon={Copy} size="xs" />
        </Button>
    </div>
);
