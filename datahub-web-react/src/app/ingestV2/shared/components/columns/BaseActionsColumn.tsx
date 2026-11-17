import { Button, Menu } from '@components';
import { DotsThreeVertical } from '@phosphor-icons/react/dist/csr/DotsThreeVertical';
import React from 'react';
import styled from 'styled-components';

import { ItemType } from '@components/components/Menu/types';

// Targets `div` because alchemy Icon renders one; the overflow trigger is a Button and
// brings its own hover affordance.
const ActionIcons = styled.div`
    display: flex;
    align-items: center;
    justify-content: end;
    gap: 12px;

    div {
        color: ${(props) => props.theme.colors.icon};
        :hover {
            cursor: pointer;
        }
    }
`;

interface Props {
    dropdownItems?: ItemType[];
    extraActions?: React.ReactNode;
}

export default function BaseActionsColumn({ dropdownItems, extraActions }: Props) {
    return (
        <ActionIcons onClick={(e) => e.stopPropagation()}>
            {extraActions}
            <Menu items={dropdownItems} trigger={['click']}>
                <Button
                    variant="text"
                    icon={{ icon: DotsThreeVertical, weight: 'bold', size: 'xl', color: 'icon' }}
                    isCircle
                    data-testid="ingestion-more-options"
                />
            </Menu>
        </ActionIcons>
    );
}
