import React from 'react';
import { pluralize } from '../../../../../../shared/textUtil';
import { SubTitle } from './components';

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
