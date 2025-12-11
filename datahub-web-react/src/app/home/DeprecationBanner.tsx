/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Icon } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';

import PageBanner from '@app/sharedV2/PageBanner';
import { useIsThemeV2Toggleable } from '@app/useIsThemeV2';
import { PageRoutes } from '@conf/Global';

export default function DeprecationBanner() {
    const [isThemeV2Toggleable] = useIsThemeV2Toggleable();

    const linkContent = (
        <span>
            {String.fromCodePoint(128064)}&nbsp; DataHub has a new look! Preview it now under{' '}
            <Link to={`${PageRoutes.SETTINGS}/preferences`}>Appearance &gt; Try New User Experience</Link> before the
            current UI goes away with v1.3.0.
        </span>
    );
    const staticContent = (
        <>
            {String.fromCodePoint(128064)}&nbsp; DataHub has a new look! Contact your Admin to unlock the new interface
            before the current UI goes away with v1.3.0.
        </>
    );

    const content = isThemeV2Toggleable ? linkContent : staticContent;

    return (
        <PageBanner
            localStorageKey="v1UIDeprecationAnnouncement"
            icon={<Icon icon="ExclamationMark" color="red" weight="fill" source="phosphor" />}
            content={content}
        />
    );
}
