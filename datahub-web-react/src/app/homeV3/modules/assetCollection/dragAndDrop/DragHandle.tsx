/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Icon } from '@components';
import { SyntheticListenerMap } from '@dnd-kit/core/dist/hooks/utilities';
import React from 'react';
import styled from 'styled-components';

const DragIcon = styled(Icon)<{ $isDragging?: boolean }>`
    cursor: ${(props) => (props.$isDragging ? 'grabbing' : 'grab')};
`;

type Props = {
    isDragging?: boolean;
    listeners?: SyntheticListenerMap;
};

export default function DragHandle({ isDragging, listeners }: Props) {
    return (
        <DragIcon
            {...listeners}
            size="lg"
            color="gray"
            icon="DotsSixVertical"
            source="phosphor"
            $isDragging={isDragging}
        />
    );
}
