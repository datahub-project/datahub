import React from 'react';

import { ConnectionCreateButton } from '@app/connections/components/CreateButton';
import { ConnectionForm } from '@app/connections/components/Form';
import { ConnectionLogo } from '@app/connections/components/Logo';
import { ConnectionSelectOrCreate } from '@app/connections/components/SelectOrCreate';
import { ConnectionsTable } from '@app/connections/components/Table';

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
