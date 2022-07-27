import React from 'react';
import ActionDropdown from './ActionDropdown';

type Props = {
    urns: Array<string>;
    disabled: boolean;
};

// eslint-disable-next-line
export default function DeprecationDropdown({ urns, disabled = false }: Props) {
    return (
        <ActionDropdown
            name="Deprecation"
            actions={[
                {
                    title: 'Mark as deprecated',
                    onClick: () => null,
                },
                {
                    title: 'Mark as undeprecated',
                    onClick: () => null,
                },
            ]}
            disabled={disabled}
        />
    );
}
