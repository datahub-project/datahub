import React from 'react';
import ActionDropdown from './ActionDropdown';

type Props = {
    urns: Array<string>;
    disabled: boolean;
};

export default function DeprecationDropdown({ urns, disabled = false }: Props) {
    console.log(urns);
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
