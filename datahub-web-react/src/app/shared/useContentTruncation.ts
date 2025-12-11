/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect, useRef, useState } from 'react';

export default function useParentContainersTruncation(dataDependency: any) {
    const contentRef = useRef<HTMLDivElement>(null);
    const [isContentTruncated, setAreContainersTruncated] = useState(false);

    useEffect(() => {
        if (contentRef && contentRef.current && contentRef.current.scrollWidth > contentRef.current.clientWidth) {
            setAreContainersTruncated(true);
        }
    }, [dataDependency]);

    return { contentRef, isContentTruncated };
}
