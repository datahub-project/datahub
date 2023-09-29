import React, { useState } from 'react';
import { Button, Form, Input, message, Modal, Table } from 'antd';
import TextArea from 'antd/lib/input/TextArea';
import { PlusOutlined } from '@ant-design/icons';
import arrow from '../../../../../../images/Arrow.svg';
import './CreateJoinModal.less';
import { EntityType, Join, OwnerEntityType } from '../../../../../../types.generated';
import { useCreateJoinMutation, useUpdateJoinMutation } from '../../../../../../graphql/join.generated';
import { useUserContext } from '../../../../../context/useUserContext';
import { EditableRow } from './EditableRow';
import { EditableCell } from './EditableCell';
import { checkDuplicateJoin, getDatasetName, JoinDataType, validateJoin } from './JoinUtils';
import { useGetSearchResultsQuery } from '../../../../../../graphql/search.generated';
import { useAddOwnerMutation } from '../../../../../../graphql/mutations.generated';

type Props = {
    table1?: any;
    table1Schema?: any;
    table2?: any;
    table2Schema?: any;
    visible: boolean;
    setModalVisible?: any;
    onCancel: () => void;
    editJoin?: Join;
    isEditing?: boolean;
    refetch: () => Promise<any>;
};

type EditableTableProps = Parameters<typeof Table>[0];
type ColumnTypes = Exclude<EditableTableProps['columns'], undefined>;

export const CreateJoinModal = ({
    table1,
    table1Schema,
    table2,
    table2Schema,
    visible,
    setModalVisible,
    onCancel,
    editJoin,
    isEditing,
    refetch,
}: Props) => {
    const [form] = Form.useForm();
    const { user } = useUserContext();
    const ownerEntityType =
        user && user.type === EntityType.CorpGroup ? OwnerEntityType.CorpGroup : OwnerEntityType.CorpUser;
    const table1Dataset = editJoin?.properties?.datasetA || table1?.dataset;
    const table1DatasetSchema = editJoin?.properties?.datasetA || table1Schema;
    const table2Dataset = editJoin?.properties?.datasetB || table2?.dataset;
    const table2DatasetSchema = editJoin?.properties?.datasetB || table2Schema?.dataset;

    const [details, setDetails] = useState<string>(editJoin?.editableProperties?.description || '');
    const [joinName, setJoinName] = useState<string>(
        editJoin?.editableProperties?.name || editJoin?.properties?.name || editJoin?.joinId || '',
    );
    const [tableData, setTableData] = useState<JoinDataType[]>(
        editJoin?.properties?.joinFieldMapping?.fieldMappings?.map((item, index) => {
            return {
                key: index,
                field1Name: item.afield,
                field2Name: item.bfield,
            };
        }) || [
            { key: '0', field1Name: '', field2Name: '' },
            { key: '1', field1Name: '', field2Name: '' },
        ],
    );
    const [count, setCount] = useState(editJoin?.properties?.joinFieldMapping?.fieldMappings?.length || 2);
    const [createMutation] = useCreateJoinMutation();
    const [updateMutation] = useUpdateJoinMutation();
    const [addOwnerMutation] = useAddOwnerMutation();
    const { refetch: getSearchResultsJoins } = useGetSearchResultsQuery({
        skip: true,
    });

    const handleDelete = (record) => {
        const newData = tableData.filter((item) => item.key !== record.key);
        setTableData(newData);
    };
    const onCancelSelect = () => {
        Modal.confirm({
            title: `Exit`,
            className: 'cancel-modal',
            content: `Are you sure you want to exit?  The changes made to the join will not be applied.`,
            onOk() {
                onCancel?.();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };
    const createJoin = () => {
        createMutation({
            variables: {
                input: {
                    properties: {
                        dataSetA: table1Dataset?.urn || '',
                        datasetB: table2Dataset?.urn || '',
                        name: joinName,
                        joinFieldmapping: {
                                details,
                            fieldMappings: tableData.map((r) => {
                                return {
                                    afield: r.field1Name,
                                    bfield: r.field2Name,
                                };
                            }),
                        },
                        created: true,
                    },
                    editableProperties: {
                        name: joinName,
                        description: details,
                    },
                },
            },
        })
            .then(({ data }) => {
                message.loading({
                    content: 'Create...',
                    duration: 2,
                });
                setTimeout(() => {
                    refetch();
                    message.success({
                        content: `Join created!`,
                        duration: 2,
                    });
                }, 2000);
                addOwnerMutation({
                    variables: {
                        input: {
                            ownerUrn: user?.urn || '',
                            resourceUrn: data?.createJoin?.urn || '',
                            ownershipTypeUrn: 'urn:li:ownershipType:__system__technical_owner',
                            ownerEntityType: ownerEntityType || EntityType,
                        },
                    },
                });
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to create join: ${e.message || ''}`, duration: 3 });
            });
    };
    const originalJoinName = editJoin?.properties?.name;
    const updateJoin = () => {
        updateMutation({
            variables: {
                urn: editJoin?.urn || '',
                input: {
                    properties: {
                        dataSetA: table1Dataset?.urn || '',
                        datasetB: table2Dataset?.urn || '',
                        name: originalJoinName || '',
                        createdBy: editJoin?.properties?.createdActor?.urn || user?.urn,
                        createdAt: editJoin?.properties?.createdTime || 0,
                        joinFieldmapping: {
                                details,
                            fieldMappings: tableData.map((r) => {
                                return {
                                    afield: r.field1Name,
                                    bfield: r.field2Name,
                                };
                            }),
                        },
                    },
                    editableProperties: {
                        name: joinName,
                        description: details,
                    },
                },
            },
        })
            .then(() => {
                message.loading({
                    content: 'updating...',
                    duration: 2,
                });
                setTimeout(() => {
                    refetch();
                    message.success({
                        content: `Join updated!`,
                        duration: 2,
                    });
                }, 2000);
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to update join: ${e.message || ''}`, duration: 3 });
            });
    };
    const onSubmit = async () => {
        const errors = validateJoin(joinName, tableData, editFlag, getSearchResultsJoins);
        if ((await errors).length > 0) {
            const err = (await errors).join(`, `);
            message.error(err);
            return;
        }
        if (isEditing) {
            updateJoin();
        } else {
            createJoin();
        }
        setModalVisible(false);
    };

    const table1NameBusiness = getDatasetName(table1Dataset);
    const table1NameTech = table1Dataset?.name || table1Dataset?.urn.split(',').at(1) || '';
    const table2NameBusiness = getDatasetName(table2Dataset);
    const table2NameTech = table2Dataset?.name || table2Dataset?.urn.split(',').at(1) || '';

    const handleAdd = () => {
        const newData: JoinDataType = {
            key: count,
            field1Name: '',
            field2Name: '',
        };
        setTableData([...tableData, newData]);
        setCount(count + 1);
    };
    const defaultColumns: (ColumnTypes[number] & { editable?: boolean; dataIndex: string; tableRecord?: any })[] = [
        {
            title: (
                <p className="titleContent">
                    <div className="firstRow">
                        <span className="titleNameDisplay"> {table1NameBusiness || table1NameTech}</span>
                    </div>
                    <div className="editableNameDisplay">{table1NameTech !== table1NameBusiness && table1NameTech}</div>
                </p>
            ),
            dataIndex: 'field1Name',
            tableRecord: table1DatasetSchema || {},
            editable: true,
        },
        {
            title: '',
            dataIndex: '',
            editable: false,
            render: () => <img src={arrow} alt="" />,
        },
        {
            title: (
                <p className="titleContent">
                    <div className="firstRow">
                        <span className="titleNameDisplay"> {table2NameBusiness || table2NameTech}</span>
                    </div>
                    <div className="editableNameDisplay">{table2NameTech !== table2NameBusiness && table2NameTech}</div>
                </p>
            ),
            dataIndex: 'field2Name',
            tableRecord: table2DatasetSchema || {},
            editable: true,
        },
        {
            title: 'Action',
            dataIndex: '',
            editable: false,
            render: (record) =>
                tableData.length > 1 ? (
                    <Button type="link" onClick={() => handleDelete(record)}>
                        Delete
                    </Button>
                ) : null,
        },
    ];
    const handleSave = (row: JoinDataType) => {
        const newData = [...tableData];
        const index = newData.findIndex((item) => row.key === item.key);
        const item = newData[index];
        newData.splice(index, 1, {
            ...item,
            ...row,
        });
        setTableData(newData);
    };
    const components = {
        body: {
            row: EditableRow,
            cell: EditableCell,
        },
    };

    const columns = defaultColumns.map((col) => {
        if (!col.editable) {
            return col;
        }
        return {
            ...col,
            onCell: (record: JoinDataType) => ({
                record,
                editable: col.editable,
                dataIndex: col.dataIndex,
                tableRecord: col.tableRecord,
                title: col.title,
                handleSave,
            }),
        };
    });
    return (
        <Modal
            title={
                <div className="footer-parent-div">
                    <p className="join-title">Join parameters</p>
                    <div>
                        <Button onClick={onCancelSelect} className="cancel-btn" size="large">
                            Cancel
                        </Button>
                    </div>
                    <div>
                        <Button className="submit-btn" size="large" id="continueButton" onClick={onSubmit}>
                            Submit
                        </Button>
                    </div>
                </div>
            }
            visible={visible}
            closable={false}
            className="CreateJoinModal"
            okButtonProps={{ hidden: true }}
            cancelButtonProps={{ hidden: true }}
            onCancel={onCancelSelect}
            destroyOnClose
        >
            <div className="inner-div">
                <p className="all-table-heading">Table 1</p>
                <p className="all-information">{table1NameBusiness}</p>
                <div className="techNameDisplay">{table1NameTech !== table1NameBusiness && table1NameTech}</div>
                <p className="all-content-heading">Table 2</p>
                <p className="all-information">{table2NameBusiness}</p>
                <div className="techNameDisplay">{table2NameTech !== table2NameBusiness && table2NameTech}</div>
                <p className="all-content-heading">Join name</p>
                <Form
                    form={form}
                    layout="vertical"
                    fields={[
                        { name: 'joinNameForm', value: joinName },
                        { name: 'joinDetails', value: details },
                    ]}
                    onFinish={onSubmit}
                >
                    <Form.Item
                        style={{ margin: 0 }}
                        name="joinNameForm"
                        rules={[
                            {
                                required: true,
                                message: `Join name is required.`,
                            },
                            {
                                validator: (_, value) =>
                                    checkDuplicateJoin(getSearchResultsJoins, value?.trim()).then((result) => {
                                        return result === true && !isEditing
                                            ? Promise.reject(
                                                  new Error(
                                                      'This join name already exists. A unique name for each join is required.',
                                                  ),
                                              )
                                            : Promise.resolve();
                                    }),
                            },
                        ]}
                    >
                        <Input size="large" className="join-name" onChange={(e) => setJoinName(e.target.value)} />
                    </Form.Item>
                    <p className="all-content-heading">Fields</p>
                    <Table
                        bordered
                        components={components}
                        dataSource={tableData}
                        className="JoinTable"
                        columns={columns as ColumnTypes}
                        pagination={false}
                    />
                    <Button type="link" className="add-btn-link" onClick={handleAdd}>
                        <PlusOutlined /> Add Row
                    </Button>
                    <p className="all-content-heading">Join details</p>
                    <Form.Item style={{ margin: 0 }} name="joinDetails">
                        <TextArea
                            className="join-details-ta"
                            placeholder="Please enter join details here"
                            onChange={(e) => setDetails(e.target.value)}
                        />
                    </Form.Item>
                </Form>
            </div>
        </Modal>
    );
};
