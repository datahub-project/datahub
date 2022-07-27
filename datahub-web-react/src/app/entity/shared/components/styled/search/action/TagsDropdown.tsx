import React from 'react';
import ActionDropdown from './ActionDropdown';

type Props = {
    urns: Array<string>;
    disabled: boolean;
};

export default function TagsDropdown({ urns, disabled = false }: Props) {
    console.log(urns);
    console.log(disabled);
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
