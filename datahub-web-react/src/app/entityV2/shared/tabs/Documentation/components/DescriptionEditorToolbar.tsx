/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import TabToolbar from '@app/entityV2/shared/components/styled/TabToolbar';
import { Button } from '@src/alchemy-components';

const StyledTabToolbar = styled(TabToolbar)`
    justify-content: flex-end !important;
    gap: 16px;
`;

type DescriptionEditorToolbarProps = {
    disableSave: boolean;
    onSave: () => void;
    onCancel: () => void;
};

export const DescriptionEditorToolbar = ({ disableSave, onSave, onCancel }: DescriptionEditorToolbarProps) => {
    return (
        <StyledTabToolbar>
            <Button variant="text" color="gray" data-testid="description-editor-cancel-button" onClick={onCancel}>
                Cancel
            </Button>
            <Button data-testid="description-editor-save-button" onClick={onSave} disabled={disableSave}>
                Publish
            </Button>
        </StyledTabToolbar>
    );
};
