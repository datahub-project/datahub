import '@app/entity/shared/components/styled/ERModelRelationship/ERModelRelationPreview.less';

import { RightOutlined } from '@ant-design/icons';
import { Button, Row, Table } from 'antd';
import React, { useState } from 'react';

import { CreateERModelRelationModal } from '@app/entity/shared/components/styled/ERModelRelationship/CreateERModelRelationModal';
import { getDatasetName } from '@app/entity/shared/components/styled/ERModelRelationship/ERModelRelationUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, ErModelRelationship, ErModelRelationshipCardinality } from '@types';

import arrow from '@images/Arrow.svg';
import editIcon from '@images/editIconBlack.svg';

type ERModelRelationRecord = {
    sourceField: string;
    destinationField: string;
};
type Props = {
    ermodelrelationData: ErModelRelationship;
    baseEntityUrn?: any;
    prePageType?: string;
    refetch: () => Promise<any>;
};
type EditableTableProps = Parameters<typeof Table>[0];
type ColumnTypes = Exclude<EditableTableProps['columns'], undefined>;
export const ERModelRelationPreview = ({ ermodelrelationData, baseEntityUrn, prePageType, refetch }: Props) => {
    const entityRegistry = useEntityRegistry();
    const handleViewEntity = (entityType, urn) => {
        const entityUrl = entityRegistry.getEntityUrl(entityType, urn);
        window.open(entityUrl, '_blank');
    };
    const entityName = entityRegistry.getEntityName(EntityType.ErModelRelationship);
    const [modalVisible, setModalVisible] = useState(false);
    const shuffleFlag =
        prePageType === 'Dataset' ? !(baseEntityUrn === ermodelrelationData?.properties?.source?.urn) : false;
    const table1EditableName = shuffleFlag
        ? getDatasetName(ermodelrelationData?.properties?.destination)
        : getDatasetName(ermodelrelationData?.properties?.source);
    const table2EditableName = shuffleFlag
        ? getDatasetName(ermodelrelationData?.properties?.source)
        : getDatasetName(ermodelrelationData?.properties?.destination);
    const table1Name =
        shuffleFlag && prePageType !== 'ERModelRelationship'
            ? ermodelrelationData?.properties?.destination?.name
            : ermodelrelationData?.properties?.source?.name;
    const table2Name =
        shuffleFlag && prePageType !== 'ERModelRelationship'
            ? ermodelrelationData?.properties?.source?.name
            : ermodelrelationData?.properties?.destination?.name;
    const table1Urn =
        shuffleFlag && prePageType !== 'ERModelRelationship'
            ? ermodelrelationData?.properties?.destination?.urn
            : ermodelrelationData?.properties?.source?.urn;
    const table2Urn =
        shuffleFlag && prePageType !== 'ERModelRelationship'
            ? ermodelrelationData?.properties?.source?.urn
            : ermodelrelationData?.properties?.destination?.urn;
    const ermodelrelationHeader =
        ermodelrelationData?.editableProperties?.name || ermodelrelationData?.properties?.name || '';
    function getFieldMap(): ERModelRelationRecord[] {
        const newData = [] as ERModelRelationRecord[];
        if (shuffleFlag && prePageType !== 'ERModelRelationship') {
            ermodelrelationData?.properties?.relationshipFieldMappings?.map((item) => {
                return newData.push({
                    sourceField: item.destinationField,
                    destinationField: item.sourceField,
                });
            });
        } else {
            ermodelrelationData?.properties?.relationshipFieldMappings?.map((item) => {
                return newData.push({
                    sourceField: item.sourceField,
                    destinationField: item.destinationField,
                });
            });
        }
        return newData;
    }
    const columns = [
        {
            title: (
                <p>
                    <div className="firstRow">
                        <div className="titleNameDisplay"> {table1EditableName || table1Name}</div>
                        <div>
                            {prePageType === 'ERModelRelationship' && (
                                <Button
                                    type="link"
                                    className="div-view-dataset"
                                    onClick={() => handleViewEntity(EntityType.Dataset, table1Urn)}
                                >
                                    View dataset <RightOutlined />
                                </Button>
                            )}
                        </div>
                    </div>

                    <div className="editableNameDisplay">{table1Name !== table1EditableName && table1Name}</div>
                </p>
            ),
            dataIndex: 'sourceField',
            width: '48%',
            sorter: ({ sourceField: a }, { sourceField: b }) => a.localeCompare(b),
        },
        {
            title: '',
            dataIndex: '',
            width: '4%',
            render: () => <img src={arrow} alt="" />,
        },
        {
            title: (
                <p>
                    <div className="firstRow">
                        <div className="titleNameDisplay"> {table2EditableName || table2Name}</div>
                        <div>
                            <Button
                                type="link"
                                className="div-view-dataset"
                                onClick={() => handleViewEntity(EntityType.Dataset, table2Urn)}
                            >
                                View dataset <RightOutlined />
                            </Button>
                        </div>
                    </div>
                    <div className="editableNameDisplay">{table2Name !== table2EditableName && table2Name}</div>
                </p>
            ),
            width: '48%',
            dataIndex: 'destinationField',
            sorter: ({ destinationField: a }, { destinationField: b }) => a.localeCompare(b),
        },
    ];

    const adjustCardinality = (cardinality) => {
        // Reverse the cardinality if the source and destination are reversed
        // Only for one_n and n_one, rest are same
        if (shuffleFlag && prePageType !== 'ERModelRelationship') {
            if (cardinality === ErModelRelationshipCardinality.OneN) return ErModelRelationshipCardinality.NOne;
            if (cardinality === ErModelRelationshipCardinality.NOne) return ErModelRelationshipCardinality.OneN;
        }
        return cardinality;
    };

    return (
        <div className="ERModelRelationPreview">
            {ermodelrelationData?.properties?.relationshipFieldMappings !== undefined && (
                <CreateERModelRelationModal
                    open={modalVisible}
                    setModalVisible={setModalVisible}
                    onCancel={() => {
                        setModalVisible(false);
                    }}
                    editERModelRelation={ermodelrelationData}
                    isEditing
                    refetch={refetch}
                    entityName={entityName}
                />
            )}
            <div className="preview-main-div">
                <div>
                    {(prePageType === 'Dataset' || ermodelrelationHeader !== ermodelrelationData?.properties?.name) && (
                        <Row>
                            <p className="all-table-heading">{ermodelrelationHeader}</p>
                            {prePageType === 'Dataset' && (
                                <div className="div-view">
                                    <Button
                                        type="link"
                                        onClick={() =>
                                            handleViewEntity(EntityType.ErModelRelationship, ermodelrelationData?.urn)
                                        }
                                    >
                                        View {entityName} <RightOutlined />{' '}
                                    </Button>
                                </div>
                            )}
                        </Row>
                    )}
                </div>
                <div>
                    <Button
                        type="link"
                        className="btn-edit"
                        onClick={() => {
                            setModalVisible(true);
                        }}
                    >
                        <div className="div-edit-img">
                            <img src={editIcon} alt="" /> <div className="div-edit">Edit {entityName}</div>
                            {prePageType === 'ERModelRelationship' && <div className="extra-margin-rev" />}
                        </div>
                    </Button>
                </div>
            </div>
            {prePageType === 'Dataset' && (
                <div className="cardinality-div">
                    <span>
                        <b>Cardinality: </b>
                        {adjustCardinality(ermodelrelationData?.properties?.cardinality)}
                    </span>
                </div>
            )}
            <Row>
                <Table
                    bordered
                    dataSource={getFieldMap()}
                    className="ERModelRelationTable"
                    columns={columns as ColumnTypes}
                    pagination={false}
                />
            </Row>
            {prePageType === 'Dataset' && (
                <div>
                    <p className="all-content-heading">{entityName} details</p>
                    <p className="all-content-info">{ermodelrelationData?.editableProperties?.description}</p>
                </div>
            )}
        </div>
    );
};
