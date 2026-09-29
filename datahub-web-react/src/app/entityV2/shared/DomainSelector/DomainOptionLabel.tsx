import React from 'react';
import styled from 'styled-components';

import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { Domain } from '@src/types.generated';

const Row = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
`;

type Props = {
    option: NestedSelectOption;
    /** Icon slot size in px — the default suits dropdown rows; selected-value slots want smaller. */
    iconSize?: number;
    iconFontSize?: number;
};

const DomainOptionLabel: React.FC<Props> = ({ option, iconSize = 20, iconFontSize = 10 }) => {
    const domain = option.entity as Domain | undefined;
    return (
        <Row>
            {domain && <DomainColoredIcon domain={domain} size={iconSize} fontSize={iconFontSize} />}
            <span>{option.label}</span>
        </Row>
    );
};

export default DomainOptionLabel;
