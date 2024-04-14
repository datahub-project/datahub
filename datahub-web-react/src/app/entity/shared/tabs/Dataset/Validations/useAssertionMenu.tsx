import React from 'react';
import CopyUrnMenuItem from '../../../../../shared/share/items/CopyUrnMenuItem';

export default function useAssertionMenu(urn: string) {
    const items = [
        {
            key: 1,
            label: <CopyUrnMenuItem key="1" urn={urn} type="Assertion" />,
        },
    ];
    return { items };
}
