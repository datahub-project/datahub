/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { CheckOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';

import TabToolbar from '@app/entity/shared/components/styled/TabToolbar';

type DescriptionEditorToolbarProps = {
    disableSave: boolean;
    onClose: () => void;
    onSave: () => void;
};

export const DescriptionEditorToolbar = ({ disableSave, onClose, onSave }: DescriptionEditorToolbarProps) => {
    return (
        <TabToolbar>
            <Button type="text" onClick={onClose}>
                Back
            </Button>
            <Button data-testid="description-editor-save-button" onClick={onSave} disabled={disableSave}>
                <CheckOutlined /> Save
            </Button>
        </TabToolbar>
    );
};
