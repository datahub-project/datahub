import React from 'react';
import ActionDropdown from './ActionDropdown';

type Props = {
    urns: Array<string>;
    disabled: boolean;
};

// eslint-disable-next-line
export default function DeleteDropdown({ urns, disabled = false }: Props) {
    return (
        <ActionDropdown
            name="Delete"
            actions={[
                {
                    title: 'Mark as deleted',
                    onClick: () => null,
                },
                {
                    title: 'Mark as undeleted',
                    onClick: () => null,
                },
            ]}
            disabled={disabled}
        />
    );
}
