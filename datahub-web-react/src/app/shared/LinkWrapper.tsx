/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import { Link, LinkProps } from 'react-router-dom';

/**
 * Wrapper around React Router's Link component to support relative AND absolute URLs. If it is an external URL,
 * a normal HTML anchor tag will be rendered. If no URL is present, only the contents will be rendered to avoid
 * displaying blank anchor tags.
 */
export const LinkWrapper = ({ to, children, ...props }: LinkProps) => {
    const regex = /^http/;
    const isExternalLink = regex.test(to);

    if (!to) return <>{children}</>;

    return isExternalLink ? (
        <a href={to} rel="noreferrer noopener" {...props}>
            {children}
        </a>
    ) : (
        <Link to={to} {...props}>
            {children}
        </Link>
    );
};
