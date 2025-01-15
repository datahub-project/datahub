import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useHistory } from 'react-router';
import { Button, Drawer } from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';
import styled from 'styled-components';
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
import { useGetLineageTimeParams } from './utils/useGetLineageTimeParams';
import analytics, { EventType } from '../analytics';
import LineageLoadingSection from './LineageLoadingSection';

const DEFAULT_DISTANCE_FROM_TOP = 106;

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
    const [fineGrainedMapForSiblings] = useState<any>({});

    const entityRegistry = useEntityRegistry();
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const showColumns = useIsShowColumnsMode();
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();

    const { loading, error, data, refetch } = useGetEntityLineageQuery({
        variables: {
            urn,
            separateSiblings: isHideSiblingMode,
            showColumns,
            startTimeMillis,
            endTimeMillis,
        },
    });

    const entityData: EntityAndType | null | undefined = useMemo(() => getEntityAndType(data), [data]);

    const [isDrawerVisible, setIsDrawVisible] = useState(false);
    const [selectedEntity, setSelectedEntity] = useState<EntitySelectParams | undefined>(undefined);
    const [asyncEntities, setAsyncEntities] = useState<FetchedEntities>(new Map());

    // In the case that any URL params change, we want to reset asyncEntities. If new parameters are added,
    // they should be added to the dependency array below.
    useEffect(() => {
        setAsyncEntities(new Map());
        // this can also be our hook for emitting the tracking event

        analytics.event({
            type: EventType.VisualLineageViewEvent,
            entityType: entityData?.type,
        });
    }, [isHideSiblingMode, startTimeMillis, endTimeMillis, entityData?.type]);

    useEffect(() => {
        if (showColumns) {
            setAsyncEntities(new Map());
        }
    }, [showColumns]);

    const drawerRef: React.MutableRefObject<HTMLDivElement | null> = useRef(null);

    const maybeAddAsyncLoadedEntity = useCallback(
        (entityAndType: EntityAndType) => {
            if (entityAndType?.entity?.urn && !asyncEntities.get(entityAndType?.entity?.urn)?.fullyFetched) {
                // record that we have added this entity
                let newAsyncEntities = extendAsyncEntities(
                    fineGrainedMap,
                    fineGrainedMapForSiblings,
                    asyncEntities,
                    entityRegistry,
                    entityAndType,
                    true,
                );
                const config = entityRegistry.getLineageVizConfig(entityAndType.type, entityAndType.entity);

                config?.downstreamChildren
                    ?.filter((child) => child.type)
                    ?.forEach((downstream) => {
                        newAsyncEntities = extendAsyncEntities(
                            fineGrainedMap,
                            fineGrainedMapForSiblings,
                            newAsyncEntities,
                            entityRegistry,
                            downstream,
                            false,
                        );
                    });
                config?.upstreamChildren
                    ?.filter((child) => child.type)
                    ?.forEach((downstream) => {
                        newAsyncEntities = extendAsyncEntities(
                            fineGrainedMap,
                            fineGrainedMapForSiblings,
                            newAsyncEntities,
                            entityRegistry,
                            downstream,
                            false,
                        );
                    });
                setAsyncEntities(newAsyncEntities);
            }
        },
        [asyncEntities, setAsyncEntities, entityRegistry, fineGrainedMap, fineGrainedMapForSiblings],
    );

    // set asyncEntity to have fullyFetched: false so we can update it in maybeAddAsyncLoadedEntity
    function resetAsyncEntity(entityUrn: string) {
        const newAsyncEntities = new Map(asyncEntities);
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        newAsyncEntities.set(entityUrn, { ...asyncEntities.get(entityUrn)!, fullyFetched: false });
        setAsyncEntities(newAsyncEntities);
    }

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
            {loading && <LineageLoadingSection />}
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
                            resetAsyncEntity(asyncData.entity.urn);
                            maybeAddAsyncLoadedEntity(asyncData);
                            analytics.event({
                                type: EventType.VisualLineageExpandGraphEvent,
                                targetEntityType: asyncData?.type,
                            });
                        }}
                        refetchCenterNode={() => {
                            refetch().then(() => {
                                resetAsyncEntity(urn);
                            });
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
                open={isDrawerVisible}
                width={490}
                bodyStyle={{ overflowX: 'hidden' }}
                mask={false}
                footer={
                    selectedEntity && (
                        <FooterButtonGroup>
                            <Button onClick={handleClose} type="text">
                                Close
                            </Button>
                            {selectedEntity.type !== EntityType.Restricted && (
                                <Button
                                    href={`${entityRegistry.getEntityUrl(selectedEntity.type, selectedEntity.urn)}`}
                                >
                                    <InfoCircleOutlined /> View details
                                </Button>
                            )}
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
