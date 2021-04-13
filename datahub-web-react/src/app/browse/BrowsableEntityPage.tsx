import * as React from 'react';
import { Affix } from 'antd';
import { SearchablePage } from '../search/SearchablePage';
import { BrowsePath } from './BrowsePath';
import { useGetBrowsePathsQuery } from '../../graphql/browse.generated';
import { EntityType } from '../../types.generated';

interface Props {
    urn: string;
    type: EntityType;
    children: React.ReactNode;
    lineageSupported?: boolean;
}

/**
 * A entity-details page that includes a search header & entity browse path view
 */
export const BrowsableEntityPage = ({ urn: _urn, type: _type, children: _children, lineageSupported }: Props) => {
    const { data } = useGetBrowsePathsQuery({ variables: { input: { urn: _urn, type: _type } } });

    return (
        <SearchablePage>
            {data && data.browsePaths && data.browsePaths.length > 0 && (
                <Affix offsetTop={80}>
                    <BrowsePath
                        type={_type}
                        path={data.browsePaths[0].path}
                        lineageSupported={lineageSupported}
                        isProfilePage
                    />
                </Affix>
            )}
            {_children}
        </SearchablePage>
    );
};
