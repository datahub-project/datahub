/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { SubTitle } from '@app/entity/shared/containers/profile/sidebar/FormInfo/components';
import { pluralize } from '@app/shared/textUtil';

interface Props {
    numRemaining: number;
}

export default function RequiredPromptsRemaining({ numRemaining }: Props) {
    return (
        <SubTitle addMargin>
            {numRemaining} required {pluralize(numRemaining, 'question', 's')} remaining
        </SubTitle>
    );
}
