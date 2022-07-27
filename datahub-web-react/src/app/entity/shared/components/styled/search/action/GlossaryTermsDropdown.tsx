import React from 'react';
import ActionDropdown from './ActionDropdown';

type Props = {
    urns: Array<string>;
    disabled: boolean;
};

// eslint-disable-next-line
export default function GlossaryTermsDropdown({ urns, disabled = false }: Props) {
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
