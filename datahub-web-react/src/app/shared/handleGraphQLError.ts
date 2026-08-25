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
        const { extensions, message: serverMessage } = graphQLErrors[0];
        const errorCode = extensions && (extensions.code as number);
        const errorSource = extensions && (extensions.errorSource as string);
        if (errorCode === ErrorCodes.Forbidden) {
            toast.error(permissionMessage);
            return;
        }
        if (errorCode === ErrorCodes.BadRequest && errorSource === 'VALIDATION') {
            // The server marked this BAD_REQUEST as validation-originated: its message is the
            // specific validator text (rule names, offending values), strictly more useful than
            // any generic override the caller passed — so the server message takes precedence.
            toast.error(serverMessage?.trim() || badRequestMessage || defaultMessage);
            return;
        }
        if (errorCode === ErrorCodes.BadRequest) {
            // Non-validation 400s keep master's behavior: only caller-supplied text is shown.
            // Verbatim server messages are gated on errorSource=VALIDATION above so arbitrary
            // BadRequest internals never leak to the toast.
            toast.error(badRequestMessage || defaultMessage);
            return;
        }
        if (errorCode === ErrorCodes.ServerError && serverErrorMessage) {
            toast.error(serverErrorMessage);
            return;
        }
    }
    toast.error(defaultMessage);
}
