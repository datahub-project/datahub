import { ErrorResponse } from '@apollo/client/link/error';
import { toast } from '@components';
import i18next from 'i18next';

import { ErrorCodes } from '@app/shared/constants';

interface Props {
    error: ErrorResponse;
    defaultMessage: string;
    permissionMessage?: string;
    badRequestMessage?: string;
    serverErrorMessage?: string;
}

export default function handleGraphQLError({
    error,
    defaultMessage,
    permissionMessage = i18next.t('shared.error:unauthorized'),
    badRequestMessage,
    serverErrorMessage,
}: Props) {
    // destroy the default error message from errorLink in App.tsx
    toast.destroy();
    const { graphQLErrors } = error;
    if (graphQLErrors && graphQLErrors.length) {
        const { extensions } = graphQLErrors[0];
        const errorCode = extensions && (extensions.code as number);
        if (errorCode === ErrorCodes.Forbidden) {
            toast.error(permissionMessage);
            return;
        }
        if (errorCode === ErrorCodes.BadRequest) {
            // Prefer the caller's override, but fall back to the server's own message (e.g. a
            // validator plugin's rejection reason) rather than the generic defaultMessage below.
            toast.error(badRequestMessage || graphQLErrors[0].message || defaultMessage);
            return;
        }
        if (errorCode === ErrorCodes.ServerError && serverErrorMessage) {
            toast.error(serverErrorMessage);
            return;
        }
    }
    toast.error(defaultMessage);
}
