import React, { useState } from 'react';
import { Button, Row, Table } from 'antd';
import { RightOutlined } from '@ant-design/icons';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import arrow from '../../../../../../images/Arrow.svg';
import editIcon from '../../../../../../images/editIconBlack.svg';
import './JoinPreview.less';
import { EntityType, Join } from '../../../../../../types.generated';
import { CreateJoinModal } from './CreateJoinModal';

type JoinRecord = {
    afield: string;
    bfield: string;
};
type Props = {
    joinData: Join;
    baseEntityUrn?: any;
    prePageType?: string;
};
type EditableTableProps = Parameters<typeof Table>[0];
type ColumnTypes = Exclude<EditableTableProps['columns'], undefined>;

export const JoinPreview = ({ joinData, baseEntityUrn, prePageType }: Props) => {
    const entityRegistry = useEntityRegistry();
    const handleViewEntity = (entityType, urn) => {
        const entityUrl = entityRegistry.getEntityUrl(entityType, urn);
        window.open(entityUrl, '_blank');
    };
    const [modalVisible, setModalVisible] = useState(false);
    const shuffleFlag = !(prePageType === 'Dataset' && baseEntityUrn === joinData?.properties?.datasetA?.urn);

    function getDatasetName(datainput: any): string {
        return datainput?.editableProperties?.name || datainput?.properties?.name || datainput?.name || datainput?.urn;
    }
    const table1EditableName = shuffleFlag
        ? getDatasetName(joinData?.properties?.datasetB)
        : getDatasetName(joinData?.properties?.datasetA);
    const table2EditableName = shuffleFlag
        ? getDatasetName(joinData?.properties?.datasetA)
        : getDatasetName(joinData?.properties?.datasetB);
    const table1Name =
        shuffleFlag && prePageType !== 'Join'
            ? joinData?.properties?.datasetB?.name
            : joinData?.properties?.datasetA?.name;
    const table2Name =
        shuffleFlag && prePageType !== 'Join'
            ? joinData?.properties?.datasetA?.name
            : joinData?.properties?.datasetB?.name;
    const table1Urn =
        shuffleFlag && prePageType !== 'Join'
            ? joinData?.properties?.datasetB?.urn
            : joinData?.properties?.datasetA?.urn;
    const table2Urn =
        shuffleFlag && prePageType !== 'Join'
            ? joinData?.properties?.datasetA?.urn
            : joinData?.properties?.datasetB?.urn;
    const joinHeader = joinData?.editableProperties?.name || joinData?.properties?.name || '';
    function getFieldMap(): JoinRecord[] {
        const newData = [] as JoinRecord[];
        if (shuffleFlag && prePageType !== 'Join') {
            joinData?.properties?.joinFieldMapping?.fieldMappings?.map((item) => {
                return newData.push({
                    afield: item.bfield,
                    bfield: item.afield,
                });
            });
        } else {
            joinData?.properties?.joinFieldMapping?.fieldMappings?.map((item) => {
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
                            {prePageType === 'Join' && (
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
        <div className="JoinPreview">
            {joinData?.properties?.joinFieldMapping !== undefined && (
                <CreateJoinModal
                    visible={modalVisible}
                    setModalVisible={setModalVisible}
                    onCancel={() => {
                        setModalVisible(false);
                    }}
                    editJoin={joinData}
                    editFlag
                />
            )}
            <div className="preview-main-div">
                <div>
                    {prePageType === 'Dataset' && (
                        <Row>
                            <p className="all-table-heading">{joinHeader}</p>
                            <Button type="link" onClick={() => handleViewEntity(EntityType.Join, joinData?.urn)}>
                                <div className="div-view">
                                    View join <RightOutlined />{' '}
                                </div>
                            </Button>
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
                            <img src={editIcon} alt="" /> <div className="div-edit">Edit Join</div>
                            {prePageType === 'Join' && <div className="extra-margin-rev" />}
                        </div>
                    </Button>
                </div>
            </div>
            <Row>
                <Table
                    bordered
                    dataSource={getFieldMap()}
                    className="JoinTable"
                    columns={columns as ColumnTypes}
                    pagination={false}
                />
            </Row>
            {prePageType === 'Dataset' && (
                <Row>
                    <p className="all-content-heading">About Join</p>
                    <p className="all-content-info">{joinData?.editableProperties?.description}</p>
                </Row>
            )}
        </div>
    );
};
