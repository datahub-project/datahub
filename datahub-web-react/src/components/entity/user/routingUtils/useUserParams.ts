import { useMemo } from 'react';

import { useLocation, useParams } from 'react-router';
import { Subview } from '../Subview';

type UserPageParams = {
    urn: string;
};

const SUBVIEW_PATH_INDEX = 3;
const ITEM_PATH_INDEX = 4;

export default function useUserParams(): { subview?: Subview; item?: string; urn: string } {
    const location = useLocation();
    const { urn } = useParams<UserPageParams>();
    return useMemo(() => {
        const parts = location.pathname.split('/');
        const subview = parts[SUBVIEW_PATH_INDEX];

        return {
            urn,
            subview:
                subview && Object.values(Subview).indexOf(subview as Subview) >= 0 ? (subview as Subview) : undefined,
            item: parts[ITEM_PATH_INDEX],
        };
    }, [location.pathname, urn]);
}
