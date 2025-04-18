import { useReactiveVar } from '@apollo/client';
import { message } from 'antd';
import React, { useEffect, useState } from 'react';
import { Redirect } from 'react-router';

import analytics, { EventType } from '@app/analytics';
import { isLoggedInVar } from '@app/auth/checkAuthStatus';
import styles from '@app/auth/login.module.css';
import useGetImplicitTokensFromUrlParams from '@app/auth/useGetImplicitTokensFromUrlParams';
import { Message } from '@app/shared/Message';
import { useAppConfig } from '@app/useAppConfig';
import { PageRoutes } from '@src/conf/Global';

export const ImplicitLogIn = () => {
    const isLoggedIn = useReactiveVar(isLoggedInVar);
    const { refreshContext } = useAppConfig();

    const [loading, setLoading] = useState(true);
    const { accessToken, idToken } = useGetImplicitTokensFromUrlParams();

    useEffect(() => {
        if (!accessToken || !idToken) {
            message.error({ content: 'Failed to get tokens from your SSO provider', duration: 3 });
        }
    }, [accessToken, idToken]);

    useEffect(() => {
        if (accessToken && idToken) {
            fetch('/oidcImplicitTokenExchange', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    id_token: idToken,
                    access_token: accessToken,
                }),
            })
                .then(async (res) => {
                    if (!res.ok) {
                        const data = await res.json();
                        const error = (data && data.message) || res.status;
                        return Promise.reject(error);
                    }
                    isLoggedInVar(true);
                    refreshContext();
                    analytics.event({ type: EventType.LogInEvent });
                    return Promise.resolve();
                })
                .catch((e) => {
                    message.error(e);
                    setTimeout(() => {
                        window.location.href = PageRoutes.LOG_IN;
                    }, 1500);
                })
                .finally(() => setLoading(false));
        }
    }, [accessToken, idToken, refreshContext]);

    if (!accessToken || !idToken) {
        return <Redirect to={PageRoutes.LOG_IN} />;
    }

    // The session token will be accessed via the cookie on the Root path
    if (isLoggedIn) {
        return <Redirect to={PageRoutes.ROOT} />;
    }

    return (
        <div className={styles.login_page}>
            <div>{loading && <Message type="loading" content="Logging in..." />}</div>
        </div>
    );
};
