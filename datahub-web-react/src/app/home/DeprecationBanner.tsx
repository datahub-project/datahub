import { Icon } from '@components';
import { ExclamationMark } from '@phosphor-icons/react/dist/csr/ExclamationMark';
import React from 'react';

import PageBanner from '@app/sharedV2/PageBanner';

export default function DeprecationBanner() {
    const staticContent = (
        <>
            {String.fromCodePoint(128064)}&nbsp; DataHub has a new look! Contact your Admin to unlock the new interface
            before the current UI goes away with v1.3.0.
        </>
    );

    const content = staticContent;

    return (
        <PageBanner
            localStorageKey="v1UIDeprecationAnnouncement"
            icon={<Icon icon={ExclamationMark} color="red" weight="fill" />}
            content={content}
        />
    );
}
