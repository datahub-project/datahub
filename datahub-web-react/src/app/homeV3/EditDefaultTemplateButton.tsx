import { Button, Tooltip } from '@components';
import React, { useCallback } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';

const ButtonWrapper = styled.div`
    display: flex;
    justify-content: flex-end;
    margin-right: 42px;
`;

export default function EditDefaultTemplateButton() {
    const { setIsEditingGlobalTemplate, isEditingGlobalTemplate } = usePageTemplateContext();

    const onClick = useCallback(() => {
        setIsEditingGlobalTemplate(true);
        analytics.event({
            type: EventType.HomePageTemplateGlobalTemplateEditingStart,
        });
    }, [setIsEditingGlobalTemplate]);

    // TODO: also hide this if you don't have permissions - CH-510
    if (isEditingGlobalTemplate) return null;

    return (
        <ButtonWrapper>
            <Tooltip title="Edit the home page that users see by default">
                <Button
                    icon={{ icon: 'PencilSimpleLine', color: 'gray', source: 'phosphor' }}
                    variant="text"
                    onClick={onClick}
                />
            </Tooltip>
        </ButtonWrapper>
    );
}
