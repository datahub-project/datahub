import React, { useState } from 'react';
import { Button, Row, Table } from 'antd';
import { RightOutlined } from '@ant-design/icons';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import arrow from '../../../../../../images/Arrow.svg';
import editIcon from '../../../../../../images/editIconBlack.svg';
import './ERModelRelationPreview.less';
import { EntityType, ErModelRelationship } from '../../../../../../types.generated';
import { CreateERModelRelationModal } from './CreateERModelRelationModal';
import { getDatasetName } from './ERModelRelationUtils';

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
    const [modalVisible, setModalVisible] = useState(false);
    const shuffleFlag = !(prePageType === 'Dataset' && baseEntityUrn === ermodelrelationData?.properties?.source?.urn);
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
                />
            )}
            <div className="preview-main-div">
                <div>
                    {(prePageType === 'Dataset' || ermodelrelationHeader !== ermodelrelationData?.properties?.name) && (
                        <Row>
                            <p className="all-table-heading">{ermodelrelationHeader}</p>
                            {prePageType === 'Dataset' && (
                                <Button
                                    type="link"
                                    onClick={() =>
                                        handleViewEntity(EntityType.ErModelRelationship, ermodelrelationData?.urn)
                                    }
                                >
                                    <div className="div-view">
                                        View ER-Model-Relationship <RightOutlined />{' '}
                                    </div>
                                </Button>
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
                            <img src={editIcon} alt="" /> <div className="div-edit">Edit ER-Model-Relationship</div>
                            {prePageType === 'ERModelRelationship' && <div className="extra-margin-rev" />}
                        </div>
                    </Button>
                </div>
            </div>
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
                    <p className="all-content-heading">ER-Model-Relationship details</p>
                    <p className="all-content-info">{ermodelrelationData?.editableProperties?.description}</p>
                </div>
            )}
        </div>
    );
};
