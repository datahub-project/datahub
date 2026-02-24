import { ErrorResponse } from '@apollo/client/link/error';
import { message } from 'antd';

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
    permissionMessage = 'Unauthorized. Please contact your DataHub administrator.',
    badRequestMessage,
    serverErrorMessage,
}: Props) {
    // destroy the default error message from errorLink in App.tsx
    message.destroy();
    const { graphQLErrors } = error;
    if (graphQLErrors && graphQLErrors.length) {
        const { extensions, message: serverMessage } = graphQLErrors[0];
        const errorCode = extensions && (extensions.code as number);

        // For permission errors, use the permission message (usually a generic "contact admin" message)
        if (errorCode === ErrorCodes.Forbidden) {
            message.error(permissionMessage);
            return;
        }

        // For bad request (validation errors), prefer the server's message as it contains
        // specific validation details from validator plugins
        if (errorCode === ErrorCodes.BadRequest) {
            const errorMessage = serverMessage?.trim() || badRequestMessage || defaultMessage;
            message.error(errorMessage);
            return;
        }

        // For server errors, prefer server message if available
        if (errorCode === ErrorCodes.ServerError) {
            const errorMessage = serverMessage?.trim() || serverErrorMessage || defaultMessage;
            message.error(errorMessage);
            return;
        }

        // For any other error with a server message, use it
        if (serverMessage?.trim()) {
            message.error(serverMessage);
            return;
        }
    }
    message.error(defaultMessage);
}
