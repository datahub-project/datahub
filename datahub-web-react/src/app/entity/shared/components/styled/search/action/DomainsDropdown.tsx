import React from 'react';
import ActionDropdown from './ActionDropdown';

type Props = {
    urns: Array<string>;
    disabled: boolean;
};

// eslint-disable-next-line
export default function DomainsDropdown({ urns, disabled = false }: Props) {
    return (
        <ActionDropdown
            name="Domains"
            actions={[
                {
                    title: 'Set domain',
                    onClick: () => null,
                },
                {
                    title: 'Unset domain',
                    onClick: () => null,
                },
            ]}
            disabled={disabled}
        />
    );
}
