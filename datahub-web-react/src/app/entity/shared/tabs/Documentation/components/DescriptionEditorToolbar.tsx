import Icon, { CheckOutlined, MailOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';

import TabToolbar from '@app/entity/shared/components/styled/TabToolbar';

import SparklesIcon from '@images/sparkles.svg?react';

const ProposeButton = styled(Button)`
    margin-right: 10px;
`;

const GenerateButton = styled(Button)`
    margin-right: 10px;
`;

type DescriptionEditorToolbarProps = {
    disableSave: boolean;
    onClose: () => void;
    onSave: () => void;
    onPropose: () => void;
    onGenerate: () => void;
    showPropose: boolean;
    showGenerate: boolean;
};

export const DescriptionEditorToolbar = ({
    disableSave,
    onClose,
    onSave,
    onPropose,
    onGenerate,
    showPropose,
    showGenerate,
}: DescriptionEditorToolbarProps) => {
    return (
        <TabToolbar>
            <Button type="text" onClick={onClose}>
                Back
            </Button>
            <div>
                {showGenerate && (
                    <GenerateButton data-testid="description-editor-generate-button" onClick={onGenerate}>
                        <Icon component={SparklesIcon} /> Generate
                    </GenerateButton>
                )}
                {showPropose && (
                    <ProposeButton data-testid="propose-description" onClick={onPropose} disabled={disableSave}>
                        <MailOutlined /> Propose
                    </ProposeButton>
                )}
                <Button data-testid="description-editor-save-button" onClick={onSave} disabled={disableSave}>
                    <CheckOutlined /> Save
                </Button>
            </div>
        </TabToolbar>
    );
};
