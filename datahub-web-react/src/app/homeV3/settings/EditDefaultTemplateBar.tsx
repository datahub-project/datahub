/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Button, Icon, colors } from '@components';
import React, { useCallback } from 'react';
import styled from 'styled-components';

import { ActionsBar } from '@components/components/ActionsBar/ActionsBar';

import analytics, { EventType } from '@app/analytics';
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

    const onClick = useCallback(() => {
        setIsEditingGlobalTemplate(false);
        analytics.event({
            type: EventType.HomePageTemplateGlobalTemplateEditingDone,
        });
    }, [setIsEditingGlobalTemplate]);

    if (!isEditingGlobalTemplate) return null;

    return (
        <ActionsBar dataTestId="editing-default-template-bar">
            <Warning>
                <Icon icon="ExclamationMark" color="red" weight="fill" source="phosphor" />
                <span>Editing Organization Default Home</span>
            </Warning>
            <Button onClick={onClick} data-testid="finish-editing-default-template">
                Done
            </Button>
        </ActionsBar>
    );
}
