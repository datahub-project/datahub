import React, { useState } from 'react';
import { Button, Row, Table } from 'antd';
import { RightOutlined } from '@ant-design/icons';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import arrow from '../../../../../../images/Arrow.svg';
import editIcon from '../../../../../../images/editIconBlack.svg';
import './ERModelRelationPreview.less';
import { EntityType, ErModelRelation } from '../../../../../../types.generated';
import { CreateERModelRelationModal } from './CreateERModelRelationModal';
import { getDatasetName } from './ERModelRelationUtils';

type ERModelRelationRecord = {
    afield: string;
    bfield: string;
};
type Props = {
    ermodelrelationData: ErModelRelation;
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
    const shuffleFlag = !(prePageType === 'Dataset' && baseEntityUrn === ermodelrelationData?.properties?.datasetA?.urn);
    const table1EditableName = shuffleFlag
        ? getDatasetName(ermodelrelationData?.properties?.datasetB)
        : getDatasetName(ermodelrelationData?.properties?.datasetA);
    const table2EditableName = shuffleFlag
        ? getDatasetName(ermodelrelationData?.properties?.datasetA)
        : getDatasetName(ermodelrelationData?.properties?.datasetB);
    const table1Name =
        shuffleFlag && prePageType !== 'ERModelRelation'
            ? ermodelrelationData?.properties?.datasetB?.name
            : ermodelrelationData?.properties?.datasetA?.name;
    const table2Name =
        shuffleFlag && prePageType !== 'ERModelRelation'
            ? ermodelrelationData?.properties?.datasetA?.name
            : ermodelrelationData?.properties?.datasetB?.name;
    const table1Urn =
        shuffleFlag && prePageType !== 'ERModelRelation'
            ? ermodelrelationData?.properties?.datasetB?.urn
            : ermodelrelationData?.properties?.datasetA?.urn;
    const table2Urn =
        shuffleFlag && prePageType !== 'ERModelRelation'
            ? ermodelrelationData?.properties?.datasetA?.urn
            : ermodelrelationData?.properties?.datasetB?.urn;
    const ermodelrelationHeader = ermodelrelationData?.editableProperties?.name || ermodelrelationData?.properties?.name || '';
    function getFieldMap(): ERModelRelationRecord[] {
        const newData = [] as ERModelRelationRecord[];
        if (shuffleFlag && prePageType !== 'ERModelRelation') {
            ermodelrelationData?.properties?.ermodelrelationFieldMapping?.fieldMappings?.map((item) => {
                return newData.push({
                    afield: item.bfield,
                    bfield: item.afield,
                });
            });
        } else {
            ermodelrelationData?.properties?.ermodelrelationFieldMapping?.fieldMappings?.map((item) => {
                return newData.push({
                    afield: item.afield,
                    bfield: item.bfield,
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
                            {prePageType === 'ERModelRelation' && (
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
            dataIndex: 'afield',
            width: '48%',
            sorter: ({ afield: a }, { afield: b }) => a.localeCompare(b),
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
            dataIndex: 'bfield',
            sorter: ({ bfield: a }, { bfield: b }) => a.localeCompare(b),
        },
    ];

    return (
        <div className="ERModelRelationPreview">
            {ermodelrelationData?.properties?.ermodelrelationFieldMapping !== undefined && (
                <CreateERModelRelationModal
                    visible={modalVisible}
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
                                <Button type="link" onClick={() => handleViewEntity(EntityType.Ermodelrelation, ermodelrelationData?.urn)}>
                                    <div className="div-view">
                                        View ermodelrelation <RightOutlined />{' '}
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
                            <img src={editIcon} alt="" /> <div className="div-edit">Edit ERModelRelation</div>
                            {prePageType === 'ERModelRelation' && <div className="extra-margin-rev" />}
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
                    <p className="all-content-heading">ERModelRelation details</p>
                    <p className="all-content-info">{ermodelrelationData?.editableProperties?.description}</p>
                </div>
            )}
        </div>
    );
};
