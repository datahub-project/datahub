import React from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { Link } from 'react-router-dom';
import { TableOutlined } from '@ant-design/icons';
import { ANTD_GRAY_V2 } from '../../shared/constants';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { EntityType, GlossaryTerm } from '../../../../types.generated';
import { IconStyleType } from '../../Entity';

const CountsContainer = styled.div`
    display: flex;
    flex-wrap: wrap;
    align-items: center;
`;

const CountContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
`;

const CountText = styled(Typography.Text)`
    color: ${ANTD_GRAY_V2[8]};
`;

const Divider = styled.div`
    height: 12px;
    border-right: 1px solid ${ANTD_GRAY_V2[5]};
    padding-left: 8px;
    margin-right: 4px;
`;

const TableIcon = styled(TableOutlined)`
    font-size: 12px;
    color: ${ANTD_GRAY_V2[8]};
`;

type Props = {
    term: GlossaryTerm;
};

const GlossaryTermCounts = ({ term }: Props) => {
    const entityRegistry = useEntityRegistry();

    const glossaryTermIcon = entityRegistry.getIcon(EntityType.GlossaryTerm, 12, IconStyleType.ACCENT, ANTD_GRAY_V2[8]);

    const relatedTermsUrl = `${entityRegistry.getEntityUrl(EntityType.GlossaryTerm, term.urn)}/${encodeURIComponent(
        'Related Terms',
    )}`;

    const getRelatedEntitiesUrl = `${entityRegistry.getEntityUrl(
        EntityType.GlossaryTerm,
        term.urn,
    )}/${encodeURIComponent('Related Entities')}`;

    return (
        <CountsContainer>
            <CountContainer>
                <TableIcon />
                <Link to={relatedTermsUrl}>
                    <CountText>related entities</CountText>
                </Link>
            </CountContainer>
            <CountContainer>
                <Divider />
                {glossaryTermIcon}
                <Link to={getRelatedEntitiesUrl}>
                    <CountText>related terms</CountText>
                </Link>
            </CountContainer>
        </CountsContainer>
    );
};

export default GlossaryTermCounts;
