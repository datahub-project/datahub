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
