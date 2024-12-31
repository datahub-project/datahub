import React from 'react';
import { ForeignKeyConstraint } from '../../../../../../../types.generated';

export const FkContext = React.createContext<{ fieldPath: string; constraint?: ForeignKeyConstraint | null } | null>(
    null,
);
