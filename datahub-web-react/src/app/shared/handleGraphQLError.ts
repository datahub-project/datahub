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
        const errorSource = extensions && (extensions.errorSource as string);

        // For permission errors, use the permission message (usually a generic "contact admin" message)
        if (errorCode === ErrorCodes.Forbidden) {
            message.error(permissionMessage);
            return;
        }

        // For validation errors specifically (marked with errorSource=VALIDATION), extract the server message
        // as it contains specific validation details from validator plugins
        if (errorCode === ErrorCodes.BadRequest && errorSource === 'VALIDATION') {
            const errorMessage = serverMessage?.trim() || badRequestMessage || defaultMessage;
            message.error(errorMessage);
            return;
        }

        // For other bad request errors (without VALIDATION source), use predefined messages
        if (errorCode === ErrorCodes.BadRequest) {
            const errorMessage = badRequestMessage || defaultMessage;
            message.error(errorMessage);
            return;
        }

        // For server errors, use only safe predefined messages (don't expose server details)
        if (errorCode === ErrorCodes.ServerError) {
            const errorMessage = serverErrorMessage || defaultMessage;
            message.error(errorMessage);
            return;
        }
    }
    message.error(defaultMessage);
}
