import * as React from 'react';
import { Affix } from 'antd';
import { LegacyBrowsePath } from './LegacyBrowsePath';
import { useGetBrowsePathsQuery } from '../../graphql/browse.generated';
import { EntityType } from '../../types.generated';

interface Props {
    urn: string;
    type: EntityType;
    children: React.ReactNode;
    lineageSupported?: boolean;
    isBrowsable?: boolean;
}

/**
 * A entity-details page that includes a search header & entity browse path view
 */
export const BrowsableEntityPage = ({
    urn: _urn,
    type: _type,
    children: _children,
    lineageSupported,
    isBrowsable,
}: Props) => {
    const { data } = useGetBrowsePathsQuery({
        variables: { input: { urn: _urn, type: _type } },
        fetchPolicy: 'cache-first',
    });

    return (
        <>
            {data && data.browsePaths && data.browsePaths.length > 0 && (
                <Affix offsetTop={60}>
                    <LegacyBrowsePath
                        type={_type}
                        path={data.browsePaths[0].path}
                        lineageSupported={lineageSupported}
                        isProfilePage
                        isBrowsable={isBrowsable}
                    />
                </Affix>
            )}
            {_children}
        </>
    );
};
