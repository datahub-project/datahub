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
import styled from 'styled-components';

import { SelectSizeOptions } from '@components/components/Select/types';

import { colors, spacing } from '@src/alchemy-components/theme';

const FooterBase = styled.div({
    display: 'flex',
    justifyContent: 'flex-end',
    gap: spacing.sm,
    paddingTop: spacing.sm,
    borderTop: `1px solid ${colors.gray[100]}`,
});

interface Props {
    onCancel?: () => void;
    onUpdate?: () => void;
    size?: SelectSizeOptions;
}

export default function DropdownFooterActions({ onCancel, onUpdate, size }: Props) {
    return (
        <FooterBase>
            <Button onClick={onCancel} variant="text" size={size} data-testid="footer-button-cancel">
                Cancel
            </Button>
            <Button
                onClick={onUpdate}
                size={size}
                onFocus={(e) => e.stopPropagation()}
                data-testid="footer-button-update"
            >
                Update
            </Button>
        </FooterBase>
    );
}
