import { Affix } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { useGetBrowsePathsQuery } from '../../../../../../graphql/browse.generated';
import { EntityType } from '../../../../../../types.generated';
import { useLineageMetadata } from '../../../EntityContext';
import { ProfileNavBrowsePath } from './ProfileNavBrowsePath';

type Props = {
    urn: string;
    entityType: EntityType;
};

const AffixWithHeight = styled(Affix)``;

export const EntityProfileNavBar = ({ urn, entityType }: Props) => {
    const { data: browseData } = useGetBrowsePathsQuery({ variables: { input: { urn, type: entityType } } });
    const lineageMetadata = useLineageMetadata();

    return (
        <AffixWithHeight offsetTop={60}>
            <ProfileNavBrowsePath
                type={entityType}
                path={browseData?.browsePaths?.[0]?.path || []}
                upstreams={lineageMetadata?.upstreamChildren?.length || 0}
                downstreams={lineageMetadata?.downstreamChildren?.length || 0}
            />
        </AffixWithHeight>
    );
};
