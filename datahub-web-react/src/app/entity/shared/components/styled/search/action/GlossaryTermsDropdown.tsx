import React from 'react';
import ActionDropdown from './ActionDropdown';

type Props = {
    urns: Array<string>;
    disabled: boolean;
};

export default function GlossaryTermsDropdown({ urns, disabled = false }: Props) {
    console.log(urns);
    return (
        <ActionDropdown
            name="Glossary Terms"
            actions={[
                {
                    title: 'Add glossary terms',
                    onClick: () => null,
                },
                {
                    title: 'Remove glossary terms',
                    onClick: () => null,
                },
            ]}
            disabled={disabled}
        />
    );
}
