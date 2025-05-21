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
