/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Affix } from 'antd';
import * as React from 'react';

import { LegacyBrowsePath } from '@app/browse/LegacyBrowsePath';

import { useGetBrowsePathsQuery } from '@graphql/browse.generated';
import { EntityType } from '@types';

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
