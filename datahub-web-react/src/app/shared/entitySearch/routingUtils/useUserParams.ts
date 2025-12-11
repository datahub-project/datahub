/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo } from 'react';
import { useLocation, useParams } from 'react-router';

type UserPageParams = {
    urn: string;
};

const SUBVIEW_PATH_INDEX = 3;
const ITEM_PATH_INDEX = 4;

export default function useUserParams(): { subview?: string; item?: string; urn: string } {
    const location = useLocation();
    const { urn } = useParams<UserPageParams>();
    return useMemo(() => {
        const parts = location.pathname.split('/');
        const subview = parts[SUBVIEW_PATH_INDEX];

        return {
            urn,
            subview,
            item: parts[ITEM_PATH_INDEX],
        };
    }, [location.pathname, urn]);
}
