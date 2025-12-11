/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Button, Icon } from '@components';
import React from 'react';

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
