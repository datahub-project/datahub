import React, { useContext } from 'react';
import styled from 'styled-components';
import { Tooltip } from '@components';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { Entity, EntityType } from '../../../../types.generated';
import { EntityLink } from './EntityLink';
import { EntityLinkListSkeleton } from './EntityLinkListSkeleton';
import { DefaultEmptyEntityList } from './DefaultEmptyEntityList';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { GenericEntityProperties } from '../../../entity/shared/types';
import OnboardingContext from '../../../onboarding/OnboardingContext';

const Title = styled.div<{ hasAction: boolean }>`
    ${(props) => props.hasAction && `:hover { cursor: pointer; }`}
    color: #403d5c;
    font-weight: 600;
    font-size: 16px;
    margin-bottom: 8px;
`;

const List = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const ShowMoreButton = styled.div`
    margin-top: 12px;
    padding: 0px;
    color: ${ANTD_GRAY[7]};
    :hover {
        cursor: pointer;
        color: ${ANTD_GRAY[8]};
        text-decoration: underline;
    }
`;

type Props = {
    loading: boolean;
    title?: string;
    tip?: React.ReactNode;
    entities: (Entity | GenericEntityProperties | null | undefined)[];
    showMore?: boolean;
    showMoreComponent?: React.ReactNode;
    showMoreCount?: number;
    showHealthIcon?: boolean;
    empty?: React.ReactNode;
    onClickMore?: () => void;
    onClickTitle?: () => void;
    render?: (entity: GenericEntityProperties) => React.ReactNode;
};

export const EntityLinkList = ({
    loading,
    title,
    tip,
    entities,
    showMoreComponent,
    showMore = false,
    showMoreCount,
    showHealthIcon = false,
    empty,
    onClickMore,
    onClickTitle,
    render,
}: Props) => {
    const entityRegistry = useEntityRegistryV2();
    const isEmpty = entities.length === 0 && !loading;
    const { isUserInitializing } = useContext(OnboardingContext);

    if (isUserInitializing || loading) {
        return <EntityLinkListSkeleton />;
    }

    return (
        <>
            {title && (
                <Title hasAction={onClickTitle !== undefined} onClick={onClickTitle}>
                    <Tooltip title={tip} showArrow={false} placement="right">
                        {title}
                    </Tooltip>
                </Title>
            )}
            <List data-testid="test">
                {(!isEmpty &&
                    entities.map((entity) => {
                        return (
                            <EntityLink
                                key={`${title}-${entity?.urn}`}
                                entity={
                                    entity
                                        ? entityRegistry.getGenericEntityProperties(entity.type as EntityType, entity)
                                        : null
                                }
                                render={render}
                                showHealthIcon={showHealthIcon}
                            />
                        );
                    })) || <>{empty || <DefaultEmptyEntityList />}</>}
            </List>
            {showMore && (
                <ShowMoreButton onClick={onClickMore}>
                    {showMoreComponent || (showMoreCount && <>show {showMoreCount} more</>) || <>show more</>}
                </ShowMoreButton>
            )}
        </>
    );
};
