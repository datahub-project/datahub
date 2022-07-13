import * as QueryString from 'query-string';
import { RouteComponentProps } from 'react-router-dom';
import { SEPARATE_SIBLINGS_URL_PARAM } from '../../entity/shared/siblingUtils';

export const navigateToLineageUrl = ({
    location,
    history,
    isLineageMode,
    isHideSiblingMode,
}: {
    location: {
        search: string;
        pathname: string;
    };
    history: RouteComponentProps['history'];
    isLineageMode: boolean;
    isHideSiblingMode?: boolean;
}) => {
    const parsedSearch = QueryString.parse(location.search, { arrayFormat: 'comma' });
    let newSearch: any = {
        ...parsedSearch,
        is_lineage_mode: isLineageMode,
    };
    if (isHideSiblingMode !== undefined) {
        newSearch = {
            ...newSearch,
            [SEPARATE_SIBLINGS_URL_PARAM]: isHideSiblingMode,
        };
    }
    const newSearchStringified = QueryString.stringify(newSearch, { arrayFormat: 'comma' });

    history.push({
        pathname: location.pathname,
        search: newSearchStringified,
    });
};
