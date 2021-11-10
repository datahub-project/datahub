import { Affix } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { useGetBrowsePathsQuery } from '../../../../../../graphql/browse.generated';
import { EntityType } from '../../../../../../types.generated';
import { GenericEntityProperties } from '../../../types';
import { ProfileNavBrowsePath } from './ProfileNavBrowsePath';

type Props = {
    urn: string;
    entityType: EntityType;
    entityData: GenericEntityProperties | null;
};

const AffixWithHeight = styled(Affix)``;

export const EntityProfileNavBar = ({ urn, entityData, entityType }: Props) => {
    const { data: browseData } = useGetBrowsePathsQuery({ variables: { input: { urn, type: entityType } } });

    return (
        <AffixWithHeight offsetTop={60}>
            <ProfileNavBrowsePath
                type={entityType}
                path={browseData?.browsePaths?.[0]?.path || []}
                upstreams={entityData?.upstreamLineage?.entities?.length || 0}
                downstreams={entityData?.downstreamLineage?.entities?.length || 0}
            />
        </AffixWithHeight>
    );
};
