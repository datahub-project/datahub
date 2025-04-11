import Link from 'antd/lib/typography/Link';
import React from 'react';
import styled from 'styled-components';
import PlatformContent from '../../header/PlatformContent';
import { useEntityData } from '../../../../../../entity/shared/EntityContext';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { StyledDivider } from '../FormInfo/components';
import { DatasetStatsSummarySubHeader } from '../../../../../dataset/profile/stats/stats/DatasetStatsSummarySubHeader';
import LinkOut from '../../../../../../../images/link-out.svg?react';
import FormInfo from '../FormInfo/FormInfo';

const EntityName = styled.div`
    font-size: 16px;
    font-weight: 700;
    line-height: 24px;
    margin-bottom: 8px;
`;

const EntityInfoWrapper = styled.div`
    padding-top: 20px;
`;

const StyledLink = styled(Link)`
    font-size: 14px;
    line-height: 18px;
    display: inline-flex;
    align-items: center;

    svg {
        height: 14px;
        width: 14px;
    }
`;

const FormInfoWrapper = styled.div`
    margin-top: 12px;
`;

interface Props {
    formUrn: string;
}

export default function EntityInfo({ formUrn }: Props) {
    const entityRegistry = useEntityRegistry();
    const { entityType, entityData } = useEntityData();
    const entityName = entityData ? entityRegistry.getDisplayName(entityType, entityData) : '';

    return (
        <EntityInfoWrapper>
            <PlatformContent />
            <EntityName>{entityName}</EntityName>
            <StyledLink
                href={`${entityRegistry.getEntityUrl(entityType, entityData?.urn || '')}/`}
                target="_blank"
                rel="noreferrer noopener"
            >
                View Profile <LinkOut style={{ marginLeft: '4px' }} />
            </StyledLink>
            <DatasetStatsSummarySubHeader />
            <FormInfoWrapper>
                <FormInfo formUrn={formUrn} />
            </FormInfoWrapper>
            <StyledDivider />
        </EntityInfoWrapper>
    );
}
