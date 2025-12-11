/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { colors } from '@components';
import React from 'react';

import { NameContainer } from '@app/homeV3/styledComponents';

interface Props {
    text?: string;
}

export default function ModuleName({ text }: Props) {
    return (
        <NameContainer
            ellipsis={{
                tooltip: {
                    color: 'white',
                    overlayInnerStyle: { color: colors.gray[1700] },
                    showArrow: false,
                },
            }}
        >
            {text}
        </NameContainer>
    );
}
