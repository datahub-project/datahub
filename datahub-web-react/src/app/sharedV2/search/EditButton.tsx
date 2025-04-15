import React from 'react';
import { Button, Tooltip } from '@components';

type Props = {
    setShowSelectMode: (showSelectMode: boolean) => any;
    disabled?: boolean;
};

export default function EditButton({ setShowSelectMode, disabled }: Props) {
    return (
        <Tooltip title="Edit..." showArrow={false} placement="top">
            <Button
                onClick={() => setShowSelectMode(true)}
                disabled={disabled}
                data-testid="search-results-edit-button"
                color="gray"
                size="sm"
                isCircle
                icon={{ icon: 'PencilSimple', source: 'phosphor', size: 'md' }}
            />
        </Tooltip>
    );
}
