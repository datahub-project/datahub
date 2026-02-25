import { Icon, typography } from '@components';
import { Dropdown } from 'antd';
import { ItemType } from 'antd/lib/menu/hooks/useItems';
import React from 'react';
import styled from 'styled-components';

export const MenuItem = styled.div`
    display: flex;
    padding: 5px 50px 5px 5px;
    font-size: 14px;
    font-weight: 500;
    color: ${(props) => props.theme.colors.text};
    font-family: ${typography.fonts.body};
`;

const ActionIcons = styled.div`
    display: flex;
    justify-content: end;
    gap: 12px;

    div {
        border: 1px solid ${(props) => props.theme.colors.border};
        border-radius: 200px;
        width: 24px;
        height: 24px;
        padding: 4px;
        color: ${(props) => props.theme.colors.textTertiary};
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
            <Dropdown menu={{ items: dropdownItems }} trigger={['click']}>
                <Icon icon="DotsThreeVertical" source="phosphor" data-testid="ingestion-more-options" />
            </Dropdown>
        </ActionIcons>
    );
}
