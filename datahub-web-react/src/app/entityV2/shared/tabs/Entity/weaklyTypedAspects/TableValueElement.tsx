/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

function isValidHttpUrl(string) {
    let url;

    try {
        url = new URL(string);
    } catch (_) {
        return false;
    }

    return url.protocol === 'http:' || url.protocol === 'https:';
}

export default function TableValueElement({ value }: { value: any }) {
    if (typeof value === 'boolean') {
        return <span>{String(value)}</span>;
    }
    if (typeof value === 'string') {
        if (isValidHttpUrl(value)) {
            return <a href={value}>{value}</a>;
        }
        return <span>{value}</span>;
    }
    if (typeof value === 'number') {
        return <span>{value}</span>;
    }
    return null;
}
