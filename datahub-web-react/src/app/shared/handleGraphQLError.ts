import { ErrorResponse } from '@apollo/client/link/error';
import { message } from 'antd';
import { ErrorCodes } from './constants';

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
        const { extensions } = graphQLErrors[0];
        const errorCode = extensions && (extensions.code as number);
        if (errorCode === ErrorCodes.Forbidden) {
            message.error(permissionMessage);
            return;
        }
        if (errorCode === ErrorCodes.BadRequest && badRequestMessage) {
            message.error(badRequestMessage);
            return;
        }
        if (errorCode === ErrorCodes.ServerError && serverErrorMessage) {
            message.error(serverErrorMessage);
            return;
        }
    }
    message.error(defaultMessage);
}
