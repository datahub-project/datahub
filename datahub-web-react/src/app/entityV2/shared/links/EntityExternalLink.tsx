/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { ReactNode } from 'react';

interface Props {
    url: string | null | undefined;
    children: ReactNode;
}

const EntityExternalLink: React.FC<Props> = ({ url, children }) => (
    <a href={url || undefined} target="blank">
        {children}
    </a>
);

export default EntityExternalLink;
