import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useHistory } from 'react-router';

import { Button, Drawer } from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';
import styled from 'styled-components';

import { Message } from '../shared/Message';
import { useEntityRegistry } from '../useEntityRegistry';
import CompactContext from '../shared/CompactContext';
import { EntityAndType, EntitySelectParams, FetchedEntities } from './types';
import LineageViz from './LineageViz';
import extendAsyncEntities from './utils/extendAsyncEntities';
import { EntityType } from '../../types.generated';
import { ANTD_GRAY } from '../entity/shared/constants';
import { GetEntityLineageQuery, useGetEntityLineageQuery } from '../../graphql/lineage.generated';
import { useIsSeparateSiblingsMode } from '../entity/shared/siblingUtils';
import { SHOW_COLUMNS_URL_PARAMS, useIsShowColumnsMode } from './utils/useIsShowColumnsMode';
import { ErrorSection } from '../shared/error/ErrorSection';
import usePrevious from '../shared/usePrevious';

const DEFAULT_DISTANCE_FROM_TOP = 106;

const LoadingMessage = styled(Message)`
    margin-top: 10%;
`;
const FooterButtonGroup = styled.div`
    display: flex;
    justify-content: space-between;
    margin: 12px 0;
`;

const EntityDrawer = styled(Drawer)<{ distanceFromTop: number }>`
    top: ${(props) => props.distanceFromTop}px;
    z-index: 1;
    height: calc(100vh - ${(props) => props.distanceFromTop}px);
    .ant-drawer-content-wrapper {
        border-right: 1px solid ${ANTD_GRAY[4.5]};
        box-shadow: none !important;
    }
`;

export function getEntityAndType(lineageData?: GetEntityLineageQuery) {
    if (lineageData && lineageData.entity) {
        return {
            type: lineageData.entity.type,
            entity: { ...lineageData.entity },
        } as EntityAndType;
    }
    return null;
}

type Props = {
    urn: string;
    type: EntityType;
};

export default function LineageExplorer({ urn, type }: Props) {
    const previousUrn = usePrevious(urn);
    const history = useHistory();
    const [fineGrainedMap] = useState<any>({ forward: {}, reverse: {} });

    const entityRegistry = useEntityRegistry();
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const showColumns = useIsShowColumnsMode();

    const { loading, error, data } = useGetEntityLineageQuery({
        variables: { urn, separateSiblings: isHideSiblingMode, showColumns },
    });

    const entityData: EntityAndType | null | undefined = useMemo(() => getEntityAndType(data), [data]);

    const [isDrawerVisible, setIsDrawVisible] = useState(false);
    const [selectedEntity, setSelectedEntity] = useState<EntitySelectParams | undefined>(undefined);
    const [asyncEntities, setAsyncEntities] = useState<FetchedEntities>({});

    // in the case that sibling mode changes, we want to clear out our cache of entities
    useEffect(() => {
        setAsyncEntities({});
    }, [isHideSiblingMode]);

    useEffect(() => {
        if (showColumns) {
            setAsyncEntities({});
        }
    }, [showColumns]);

    const drawerRef: React.MutableRefObject<HTMLDivElement | null> = useRef(null);

    const maybeAddAsyncLoadedEntity = useCallback(
        (entityAndType: EntityAndType) => {
            if (entityAndType?.entity.urn && !asyncEntities[entityAndType?.entity.urn]?.fullyFetched) {
                // record that we have added this entity
                let newAsyncEntities = extendAsyncEntities(
                    fineGrainedMap,
                    asyncEntities,
                    entityRegistry,
                    entityAndType,
                    true,
                );
                const config = entityRegistry.getLineageVizConfig(entityAndType.type, entityAndType.entity);

                config?.downstreamChildren?.forEach((downstream) => {
                    newAsyncEntities = extendAsyncEntities(
                        fineGrainedMap,
                        newAsyncEntities,
                        entityRegistry,
                        downstream,
                        false,
                    );
                });
                config?.upstreamChildren?.forEach((downstream) => {
                    newAsyncEntities = extendAsyncEntities(
                        fineGrainedMap,
                        newAsyncEntities,
                        entityRegistry,
                        downstream,
                        false,
                    );
                });
                setAsyncEntities(newAsyncEntities);
            }
        },
        [asyncEntities, setAsyncEntities, entityRegistry, fineGrainedMap],
    );

    const handleClose = () => {
        setIsDrawVisible(false);
        setSelectedEntity(undefined);
    };

    useEffect(() => {
        if (type && entityData && !loading) {
            maybeAddAsyncLoadedEntity(entityData);
        }
    }, [entityData, setAsyncEntities, maybeAddAsyncLoadedEntity, urn, previousUrn, type, loading]);

    const drawerDistanceFromTop =
        drawerRef && drawerRef.current ? drawerRef.current.offsetTop : DEFAULT_DISTANCE_FROM_TOP;

    return (
        <>
            {error && <ErrorSection />}
            {loading && <LoadingMessage type="loading" content="Loading..." />}
            {!!data && (
                <div>
                    <LineageViz
                        fineGrainedMap={fineGrainedMap}
                        selectedEntity={selectedEntity}
                        fetchedEntities={asyncEntities}
                        entityAndType={entityData}
                        onEntityClick={(params: EntitySelectParams) => {
                            setIsDrawVisible(true);
                            setSelectedEntity(params);
                        }}
                        onEntityCenter={(params: EntitySelectParams) => {
                            history.push(
                                `${entityRegistry.getEntityUrl(
                                    params.type,
                                    params.urn,
                                )}/?is_lineage_mode=true&${SHOW_COLUMNS_URL_PARAMS}=${showColumns}`,
                            );
                        }}
                        onLineageExpand={(asyncData: EntityAndType) => {
                            maybeAddAsyncLoadedEntity(asyncData);
                        }}
                    />
                </div>
            )}
            <div ref={drawerRef} />
            <EntityDrawer
                distanceFromTop={drawerDistanceFromTop}
                placement="left"
                closable={false}
                onClose={handleClose}
                visible={isDrawerVisible}
                width={490}
                mask={false}
                footer={
                    selectedEntity && (
                        <FooterButtonGroup>
                            <Button onClick={handleClose} type="text">
                                Close
                            </Button>
                            <Button href={entityRegistry.getEntityUrl(selectedEntity.type, selectedEntity.urn)}>
                                <InfoCircleOutlined /> {entityRegistry.getEntityName(selectedEntity.type)} Details
                            </Button>
                        </FooterButtonGroup>
                    )
                }
            >
                <CompactContext.Provider value>
                    {selectedEntity && entityRegistry.renderProfile(selectedEntity.type, selectedEntity.urn)}
                </CompactContext.Provider>
            </EntityDrawer>
        </>
    );
}
