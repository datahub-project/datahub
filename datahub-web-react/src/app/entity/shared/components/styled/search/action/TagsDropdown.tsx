import React from 'react';
import ActionDropdown from './ActionDropdown';

type Props = {
    urns: Array<string>;
    disabled: boolean;
};

// eslint-disable-next-line
export default function TagsDropdown({ urns, disabled = false }: Props) {
    return (
        <ActionDropdown
            name="Tags"
            actions={[
                {
                    title: 'Add tags',
                    onClick: () => null,
                },
                {
                    title: 'Remove tags',
                    onClick: () => null,
                },
            ]}
            disabled={disabled}
        />
    );
}
