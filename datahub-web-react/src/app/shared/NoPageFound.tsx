import React from 'react';
import { useReactiveVar } from '@apollo/client';
import { Redirect } from 'react-router';
import { isLoggedInVar } from '../auth/checkAuthStatus';
import { PageRoutes } from '../../conf/Global';

export const NoPageFound = () => {
    const isLoggedIn = useReactiveVar(isLoggedInVar);

    if (!isLoggedIn) {
        return <Redirect to={PageRoutes.LOG_IN} />;
    }

    return (
        <div>
            <p>Page Not Found!</p>
        </div>
    );
};
