import React from 'react';
import styled from 'styled-components';
import { PencilSimpleLine } from '@phosphor-icons/react';
import { Button } from 'antd';
import { colors } from '@src/alchemy-components';

const EditTestActionButton = styled(Button)`
    && {
        margin: 0px;
        padding: 0px 6px 0px 4px;
        color: ${colors.gray[1700]};
        font-size: 16px;
    }
`;

export type Props = {
    onClickEdit?: () => void;
    index?: number;
};

export default function TestCardEditButton({ onClickEdit, index }: Props) {
    return (
        <EditTestActionButton type="text" onClick={onClickEdit} data-testid={`test-edit-button-${index}`}>
            <PencilSimpleLine />
        </EditTestActionButton>
    );
}
