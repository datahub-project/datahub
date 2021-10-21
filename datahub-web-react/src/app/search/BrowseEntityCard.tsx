import React from 'react';
import { Typography } from 'antd';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { useEntityRegistry } from '../useEntityRegistry';
import { PageRoutes } from '../../conf/Global';
import { IconStyleType } from '../entity/Entity';
import { EntityType } from '../../types.generated';
import { ANTD_GRAY } from '../entity/shared/constants';
import { formatNumber } from '../shared/formatNumber';

const ContentRow = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
`;

const CountRow = styled.div``;

const TitleRow = styled.div`
    margin-top: 8px;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
`;

const CountText = styled(Typography.Text)`
    font-size: 24px;
    color: ${ANTD_GRAY[8]};
`;

const TypeText = styled(Typography.Title)`
    font-size: 18px;
    color: ${ANTD_GRAY[9]};
`;

const EntityCard = styled.div`
    margin-right: 24px;
    margin-bottom: 12px;
    width: 160px;
    height: 140px;
    display: flex;
    justify-content: center;
    border-radius: 4px;
    align-items: center;
    flex-direction: column;
    border: 1px solid ${ANTD_GRAY[4]};
    && {
        border-color: ${(props) => props.theme.styles['border-color-base']};
        box-shadow: ${(props) => props.theme.styles['box-shadow']};
    }
    &&:hover {
        box-shadow: ${(props) => props.theme.styles['box-shadow-hover']};
    }
`;

export const BrowseEntityCard = ({ entityType, count }: { entityType: EntityType; count: number }) => {
    const entityRegistry = useEntityRegistry();
    const formattedCount = formatNumber(count);
    return (
        <Link to={`${PageRoutes.BROWSE}/${entityRegistry.getPathName(entityType)}`}>
            <EntityCard>
                <ContentRow>
                    {entityRegistry.getIcon(entityType, 20, IconStyleType.HIGHLIGHT)}
                    <TitleRow>
                        <CountRow>
                            <CountText>{formattedCount}</CountText>
                        </CountRow>
                        <TypeText level={5}>
                            {`${
                                count > 1 || count <= 0
                                    ? entityRegistry.getCollectionName(entityType)
                                    : entityRegistry.getEntityName(entityType)
                            }`}{' '}
                        </TypeText>
                    </TitleRow>
                </ContentRow>
            </EntityCard>
        </Link>
    );
};
