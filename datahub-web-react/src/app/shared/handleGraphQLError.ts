import { ErrorResponse } from '@apollo/client/link/error';
import { message } from 'antd';
import { ErrorCodes } from './constants';

interface Props {
    error: ErrorResponse;
    permissionMessage: string;
    defaultMessage: string;
}

export default function handleGraphQLError({ error, permissionMessage, defaultMessage }: Props) {
    // destroy the default error message from errorLink in App.tsx
    message.destroy();
    const { graphQLErrors } = error;
    if (graphQLErrors && graphQLErrors.length) {
        const { extensions } = graphQLErrors[0];
        const errorCode = extensions && (extensions.code as number);
        if (errorCode === ErrorCodes.Unauthorized) {
            message.error(permissionMessage);
            return;
        }
    }
    message.error(defaultMessage);
}
