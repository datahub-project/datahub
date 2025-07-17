import { Button, Icon, colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import { ActionsBar } from '@components/components/ActionsBar/ActionsBar';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';

const Warning = styled.div`
    padding: 8px;
    background-color: ${colors.red[0]};
    color: ${colors.red[1000]};
    display: flex;
    align-items: center;
    gap: 8px;
    font-weight: 600;
    font-size: 14px;
    border-radius: 8px;
`;

export default function EditDefaultTemplateBar() {
    const { setIsEditingGlobalTemplate, isEditingGlobalTemplate } = usePageTemplateContext();

    // TODO: also hide this if you don't have permissions - CH-510
    if (!isEditingGlobalTemplate) return null;

    return (
        <ActionsBar>
            <Warning>
                <Icon icon="ExclamationMark" color="red" weight="fill" source="phosphor" />
                <span>Editing default user view</span>
            </Warning>
            <Button onClick={() => setIsEditingGlobalTemplate(false)}>Done</Button>
        </ActionsBar>
    );
}
