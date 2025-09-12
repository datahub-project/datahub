import { ActionsBar, Button } from '@components';
import React from 'react';

interface Props {
    onCancel: () => void;
    onUpdate: () => void;
    areActionsDisabled?: boolean;
}

export default function DescriptionActionsBar({ onCancel, onUpdate, areActionsDisabled }: Props) {
    return (
        <>
            <ActionsBar>
                <Button variant="text" color="gray" onClick={onCancel}>
                    Cancel
                </Button>
                <Button onClick={onUpdate} disabled={areActionsDisabled}>
                    Publish
                </Button>
            </ActionsBar>
        </>
    );
}
