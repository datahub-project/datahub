import { ErrorResponse } from '@apollo/client/link/error';
import { toast } from '@components';

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
    toast.destroy();
    const { graphQLErrors } = error;
    if (graphQLErrors && graphQLErrors.length) {
        const { extensions } = graphQLErrors[0];
        const errorCode = extensions && (extensions.code as number);
        if (errorCode === ErrorCodes.Forbidden) {
            toast.error(permissionMessage);
            return;
        }
        if (errorCode === ErrorCodes.BadRequest && badRequestMessage) {
            toast.error(badRequestMessage);
            return;
        }
        if (errorCode === ErrorCodes.ServerError && serverErrorMessage) {
            toast.error(serverErrorMessage);
            return;
        }
    }
    toast.error(defaultMessage);
}
