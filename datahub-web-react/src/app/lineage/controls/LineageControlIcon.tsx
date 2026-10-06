import { Icon } from '@components';
import React from 'react';
import styled from 'styled-components';

import type { IconProps } from '@components/components/Icon/types';

import { LINEAGE_CONTROL_ICON_SIZE, LINEAGE_CONTROL_ICON_WEIGHT } from '@app/lineage/controls/constants';

const IconSlot = styled.span`
    display: inline-flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    width: 20px;
    height: 20px;
    min-width: 20px;
    min-height: 20px;
`;

type Props = Omit<IconProps, 'size' | 'weight'>;

export default function LineageControlIcon(props: Props) {
    return (
        <IconSlot>
            <Icon size={LINEAGE_CONTROL_ICON_SIZE} weight={LINEAGE_CONTROL_ICON_WEIGHT} {...props} />
        </IconSlot>
    );
}
