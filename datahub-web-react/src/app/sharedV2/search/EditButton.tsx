import { Button, Tooltip } from '@components';
import { PencilSimple } from '@phosphor-icons/react/dist/ssr';
import React from 'react';

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
                icon={{ icon: PencilSimple }}
                variant="text"
                color="gray"
                size="md"
            />
        </Tooltip>
    );
}
