import { Modal, Skeleton } from 'antd';
import React, { useContext } from 'react';
import styled from 'styled-components/macro';
import { useGetEntitiesQuery } from '@src/graphql/entity.generated';
import { Entity, EntityPath, LineageDirection } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { CompactEntityNameList } from '../../recommendations/renderer/component/CompactEntityNameList';
import { getDisplayedColumns } from './ColumnPathsText';
import ColumnsRelationshipText from './ColumnsRelationshipText';
import { LineageTabContext } from '../../entityV2/shared/tabs/Lineage/LineageTabContext';

const StyledModal = styled(Modal)`
    width: 70vw;
    max-width: 850px;
`;

const PathWrapper = styled.div`
    display: inline-block;
    margin: 15px 0 15px -4px;
    padding: 20px;
    border: 1px solid ${ANTD_GRAY[4]};
    border-radius: 8px;
    box-shadow: 1px 1px 12px 4px #0000000d;
    width: 100%;
`;

const Header = styled.div`
    font-size: 16px;
    padding-top: 8px;
`;

const ErrorContainer = styled.div`
    font-size: 14px;
    padding: 8px 0px;
    color: ${ANTD_GRAY[7]};
`;

interface Props {
    paths: EntityPath[];
    resultEntityUrn: string;
    hideModal: () => void;
}

export default function EntityPathsModal({ paths, resultEntityUrn, hideModal }: Props) {
    const { lineageDirection } = useContext(LineageTabContext);
    const displayedColumns = getDisplayedColumns(paths, resultEntityUrn);

    // We need to fetch the below urns before loading the content of this modal
    const entityUrnsToFetch: string[] = paths
        .flatMap((path) => path.path ?? [])
        .map((entity) => entity?.urn)
        .filter((urn): urn is string => !!urn);
    const {
        data: result,
        loading,
        error,
    } = useGetEntitiesQuery({
        variables: { urns: entityUrnsToFetch },
    });
    const fetchedEntities = result?.entities;

    const loadedState = error ? (
        <ErrorContainer>Encountered an error while trying to fetch paths. Please try again later.</ErrorContainer>
    ) : (
        paths.map((path, i) => {
            const entities: Entity[] = (
                lineageDirection === LineageDirection.Upstream ? [...path.path].reverse() : path.path
            )
                .map(
                    (bareEntity) =>
                        fetchedEntities?.find(
                            (hydratedEntity) => hydratedEntity && hydratedEntity.urn === bareEntity?.urn,
                        ) as Entity | undefined,
                )
                .filter((entity): entity is Entity => !!entity);
            const key = `${i}`;
            return (
                <PathWrapper key={key}>
                    <CompactEntityNameList entities={entities} showArrows />
                </PathWrapper>
            );
        })
    );
    return (
        <StyledModal
            data-testid="entity-paths-modal"
            title={
                <Header>
                    Column path{paths.length > 1 && 's'} from{' '}
                    <ColumnsRelationshipText displayedColumns={displayedColumns} />
                </Header>
            }
            width="75vw"
            visible
            onCancel={hideModal}
            onOk={hideModal}
            footer={null}
            bodyStyle={{ padding: '16px 24px' }}
        >
            {loading ? <Skeleton /> : loadedState}
        </StyledModal>
    );
}
