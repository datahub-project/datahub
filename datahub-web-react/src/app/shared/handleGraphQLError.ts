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
        if (errorCode === ErrorCodes.Forbidden) {
            // Server 403 messages (e.g. which privilege is missing) are actionable for the
            // user, so surface them when present. Suppressing them is not a security measure
            // anyway — the same message is readable via the API with the user's own token.
            toast.error(serverMessage?.trim() || permissionMessage);
            return;
        }
        if (errorCode === ErrorCodes.BadRequest) {
            // 4xx messages describe the client's mistake (validator text, invalid input), so
            // the server message takes precedence over any generic caller override. Only 5xx
            // messages stay generic below — that's where internal failure detail (SQL/search
            // engine errors) lives, which would only confuse users in a toast.
            toast.error(serverMessage?.trim() || badRequestMessage || defaultMessage);
            return;
        }
        if (errorCode === ErrorCodes.ServerError && serverErrorMessage) {
            toast.error(serverErrorMessage);
            return;
        }
    }
    toast.error(defaultMessage);
}
