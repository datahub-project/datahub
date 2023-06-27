import React, { useState } from 'react';
import { Button, Row, Table } from 'antd';
import { RightOutlined } from '@ant-design/icons';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import arrow from '../../../../../../images/Arrow.svg';
import editIcon from '../../../../../../images/editIconBlack.svg';
import './JoinPreview.less';
import { EntityType, Join } from '../../../../../../types.generated';
import { CreateJoinModal } from './CreateJoinModal';

type Props = {
    joinData: Join;
    table1Name: string;
    table2Name: string;
    table1Urn: string;
    table2Urn: string;
    joinHeader: string;
    fieldMap: any;
    joinDetails?: string;
    prePageType?: string;
    shuffleFlag?: boolean;
};
type EditableTableProps = Parameters<typeof Table>[0];
type ColumnTypes = Exclude<EditableTableProps['columns'], undefined>;

export const JoinPreview = ({
    joinData,
    table1Name,
    table2Name,
    table1Urn,
    table2Urn,
    joinHeader,
    fieldMap,
    joinDetails,
    prePageType,
    shuffleFlag,
}: Props) => {
    const entityRegistry = useEntityRegistry();
    const handleViewEntity = (entityType, urn) => {
        const entityUrl = entityRegistry.getEntityUrl(entityType, urn);
        window.open(entityUrl, '_blank');
    };
    const [modalVisible, setModalVisible] = useState(false);
    function getDatasetName(datainput: any): string {
        return datainput?.editableProperties?.name || datainput?.properties?.name || datainput?.name || datainput?.urn;
    }
    const table1EditableName = shuffleFlag
        ? getDatasetName(joinData?.properties?.datasetB)
        : getDatasetName(joinData?.properties?.datasetA);
    const table2EditableName = shuffleFlag
        ? getDatasetName(joinData?.properties?.datasetA)
        : getDatasetName(joinData?.properties?.datasetB);
    const columns = [
        {
            title: (
                <p className="titleContent">
                    <div className="firstRow">
                        <span className="titleNameDisplay"> {table1EditableName || table1Name}</span>

                        {prePageType === 'Join' && (
                            <Button
                                type="link"
                                className="div-view-dataset"
                                onClick={() => handleViewEntity(EntityType.Dataset, table1Urn)}
                            >
                                View dataset <RightOutlined />{' '}
                            </Button>
                        )}
                    </div>

                    <div className="editableNameDisplay">{table1Name !== table1EditableName && table1Name}</div>
                </p>
            ),
            dataIndex: 'afield',
            sorter: ({ afield: a }, { afield: b }) => a.localeCompare(b),
            width: '40%',
        },
        {
            title: '',
            dataIndex: '',
            width: '8%',
            render: () => <img src={arrow} alt="" />,
        },
        {
            title: (
                <p className="titleContent">
                    <div className="firstRow">
                        <span className="titleNameDisplay"> {table2EditableName || table2Name}</span>
                        <Button
                            type="link"
                            className="div-view-dataset"
                            onClick={() => handleViewEntity(EntityType.Dataset, table2Urn)}
                        >
                            View dataset <RightOutlined />{' '}
                        </Button>
                    </div>
                    <div className="editableNameDisplay">{table2Name !== table2EditableName && table2Name}</div>
                </p>
            ),
            dataIndex: 'bfield',
            sorter: ({ bfield: a }, { bfield: b }) => a.localeCompare(b),
            width: '40%',
        },
    ];

    return (
        <div className="JoinPreview">
            {joinData?.properties?.joinFieldMappings !== undefined && (
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
            <Row>
                <div className="table-main-div">
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
                <Button
                    type="link"
                    className="btn-edit"
                    onClick={() => {
                        setModalVisible(true);
                    }}
                >
                    <div className="div-edit-img">
                        <img src={editIcon} alt="" /> <div className="div-edit">Edit Join</div>
                    </div>
                </Button>
            </Row>
            <Row>
                <Table
                    bordered
                    dataSource={fieldMap}
                    className="JoinTable"
                    columns={columns as ColumnTypes}
                    pagination={false}
                />
            </Row>
            <p className="all-content-heading">Join details</p>
            <p className="all-content-info">{joinDetails}</p>
        </div>
    );
};
