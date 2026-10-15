import React from 'react';

import AppProviders from '@app/AppProviders';
import { ProtectedRoutes } from '@app/ProtectedRoutes';

/**
 * Authenticated shell. Kept in its own module so `Routes` can `React.lazy` it:
 * login / signup do not download AppProviders (entity registry) or the rest of
 * the signed-in tree.
 */
export default function AuthenticatedApp() {
    return (
        <AppProviders>
            <ProtectedRoutes />
        </AppProviders>
    );
}
