import React from 'react';

import { ForeignKeyConstraint } from '@types';

export const FkContext = React.createContext<{ fieldPath: string; constraint?: ForeignKeyConstraint | null } | null>(
    null,
);
