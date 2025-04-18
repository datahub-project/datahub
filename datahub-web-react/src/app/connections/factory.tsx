import React from 'react';

import { ConnectionLogo } from './components/Logo';
import { ConnectionCreateButton } from './components/CreateButton';
import { ConnectionsTable } from './components/Table';
import { ConnectionSelectOrCreate } from './components/SelectOrCreate';
import { ConnectionForm } from './components/Form';

// Component Factory without repeated withContext calls
export const buildComponents = (constants) => {
    const { CONFIG, FIELDS } = constants;

    const components = {
        Logo: (props) => <ConnectionLogo logoUrl={CONFIG.logoUrl || ''} {...props} />,
        CreateButton: (props) => (
            <ConnectionCreateButton
                constants={constants}
                form={(formProps) => <ConnectionForm fields={FIELDS} constants={constants} {...formProps} {...props} />}
                {...props}
            />
        ),
        Table: (props) => (
            <ConnectionsTable
                constants={constants}
                form={(formProps) => <ConnectionForm fields={FIELDS} constants={constants} {...formProps} {...props} />}
                {...props}
            />
        ),
        SelectOrCreate: (props) => (
            <ConnectionSelectOrCreate
                constants={constants}
                form={(formProps) => <ConnectionForm fields={FIELDS} constants={constants} {...formProps} {...props} />}
                {...props}
            />
        ),
        Form: (props) => <ConnectionForm fields={FIELDS} constants={constants} {...props} />,
    };

    return components;
};
