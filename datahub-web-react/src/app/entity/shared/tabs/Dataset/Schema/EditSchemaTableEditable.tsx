// import { Empty } from 'antd';
import React, { useState } from 'react';
import { Button, Divider, Form, Input, Row, Select, Table, Typography } from 'antd';
import PropTypes from 'prop-types';
import axios from 'axios';
import { useBaseEntity } from '../../../EntityContext';
import { GetDatasetSchemaQuery } from '../../../../../../graphql/dataset.generated';
import { FindMyUrn, FindWhoAmI, GetMyToken } from '../../../../dataset/whoAmI';
import { WhereAmI } from '../../../../../home/whereAmI';
import { printErrorMsg, printSuccessMsg } from '../ApiCallUtils';

const { Option } = Select;

export const EditSchemaTableEditable = () => {
    const urlBase = WhereAmI();
    const publishUrl = `${urlBase}custom/update_schema`;
    console.log(`the final url is ${publishUrl}`);
    const queryFields = useBaseEntity<GetDatasetSchemaQuery>()?.dataset?.schemaMetadata?.fields;

    const urn = useBaseEntity<GetDatasetSchemaQuery>()?.dataset?.schemaMetadata?.datasetUrn;
    const currUser = FindWhoAmI();
    const currUserUrn = FindMyUrn();
    const userToken = GetMyToken(currUserUrn);
    // console.log(`user is ${currUserUrn} and token is ${userToken}, received at ${Date().toLocaleString()}`);

    const dataSource = queryFields?.map((x, ind) => {
        return {
            key: ind,
            fieldName: x?.fieldPath,
            datahubType: x?.type as string,
            nativeDataType: x?.nativeDataType as string,
            fieldDescription: x?.description as string,
            fieldTags: x?.globalTags as string[],
            fieldGlossaryTerms: x?.glossaryTerms as string[],
            editKey: ind.toString(),
        };
    });
    const formalData = dataSource || [];
    const [form] = Form.useForm();
    const [data, setData] = useState(formalData);
    const [modifiedForm, setModifiedForm] = useState(false);
    const [allrows, updateSelected] = useState({ selected: [] as any });
    const [editingKey, setEditingKey] = useState('');
    const EditableCell = ({ editing, dataIndex, title, _record, _index, children, inputType, ...restProps }) => {
        const selector = (
            <Select placeholder="Does the file contains header" data-testid="select">
                <Option value="STRING">STRING</Option>
                <Option value="NUMBER">NUMBER</Option>
                <Option value="BOOLEAN">BOOLEAN</Option>
                <Option value="TIME">TIME</Option>
                <Option value="DATE">DATE</Option>
                <Option value="BYTES">BYTES</Option>
                <Option value="NULL">NULL</Option>
                <Option value="RECORD">RECORD</Option>
                <Option value="ARRAY">ARRAY</Option>
                <Option value="UNKNOWN">UNKNOWN</Option>
            </Select>
        );
        const inputNode = inputType === 'select' ? selector : <Input />;

        return (
            <td {...restProps}>
                {editing ? (
                    <Form.Item
                        name={dataIndex}
                        style={{
                            margin: 0,
                        }}
                        rules={[
                            {
                                // because nativeDataType is "optional"
                                required: title !== 'Native DataType',
                                message: `Please Input ${title}!`,
                            },
                        ]}
                    >
                        {inputNode}
                    </Form.Item>
                ) : (
                    children
                )}
            </td>
        );
    };
    EditableCell.propTypes = {
        title: PropTypes.objectOf(PropTypes.any).isRequired,
        editing: PropTypes.objectOf(PropTypes.any).isRequired,
        children: PropTypes.objectOf(PropTypes.any).isRequired,
        inputType: PropTypes.objectOf(PropTypes.any).isRequired,
        dataIndex: PropTypes.objectOf(PropTypes.any).isRequired,
        _record: PropTypes.objectOf(PropTypes.any).isRequired,
        handleSave: PropTypes.objectOf(PropTypes.any).isRequired,
        _index: PropTypes.objectOf(PropTypes.any).isRequired,
    };

    const isEditing = (record) => record.key === editingKey;

    const edit = (record) => {
        form.setFieldsValue({
            fieldName: '',
            datahubType: '',
            nativeDataType: '',
            editKey: '',
            ...record,
        });
        setEditingKey(record.key);
    };

    const cancel = () => {
        setEditingKey('');
    };

    const save = async (key) => {
        try {
            const row = await form.validateFields();
            const newData = [...data];
            const index = newData.findIndex((item) => key === item.key);

            if (index > -1) {
                const item = newData[index];
                newData.splice(index, 1, { ...item, ...row });
                setData(newData);
                setEditingKey('');
            } else {
                newData.push(row);
                setData(newData);
                setEditingKey('');
            }
            setModifiedForm(true);
        } catch (errInfo) {
            console.log('Validate Failed:', errInfo);
        }
    };

    const columns = [
        {
            title: 'Name',
            dataIndex: 'fieldName',
            width: '45%',
            editable: true,
        },
        {
            title: 'Data Type',
            dataIndex: 'datahubType',
            width: '15%',
            editable: true,
        },
        {
            title: 'Native DataType',
            dataIndex: 'nativeDataType',
            width: '35%',
            editable: true,
        },
        {
            title: 'Edit',
            dataIndex: 'operation',
            width: '35%',
            render: (_, record) => {
                const editable = isEditing(record);
                return editable ? (
                    <span>
                        <button
                            type="button"
                            onClick={() => save(record.key)}
                            style={{
                                marginRight: 8,
                            }}
                        >
                            Save
                        </button>
                        <button
                            type="button"
                            onClick={() => cancel()}
                            style={{
                                marginRight: 8,
                            }}
                        >
                            Cancel
                        </button>
                    </span>
                ) : (
                    <Typography.Link disabled={editingKey !== ''} onClick={() => edit(record)}>
                        Edit
                    </Typography.Link>
                );
            },
        },
    ];
    const rowSelection = {
        onChange: (selectedRowKeys, selectedRows) => {
            allrows.selected = Array.from(selectedRowKeys);
            updateSelected(allrows);
            console.log(allrows.selected, selectedRows);
        },
    };
    const mergedColumns = columns.map((col) => {
        if (!col.editable) {
            return col;
        }

        return {
            ...col,
            onCell: (record) => ({
                record,
                dataIndex: col.dataIndex,
                inputType: col.dataIndex === 'datahubType' ? 'select' : 'text',
                title: col.title,
                editing: isEditing(record),
            }),
        };
    });

    const submitData = () => {
        const dataClone = data.map((x) => x);
        const dataSubmission = {
            dataset_name: urn,
            requestor: currUser,
            dataset_fields: dataClone,
            user_token: userToken,
        };
        axios
            .post(publishUrl, dataSubmission)
            .then((response) => {
                printSuccessMsg(response.status);
                window.location.reload();
            })
            .catch((error) => {
                printErrorMsg(error.toString());
            });
    };
    const deleteRow = () => {
        if (allrows.selected.length > 0) {
            setModifiedForm(true);
        }
        const removeCandidate = allrows.selected.reverse();
        const newArr = data.filter((item) => !removeCandidate.includes(item.key));
        setData(newArr);
        allrows.selected = [] as any;
        updateSelected(allrows);
    };
    function swap(arr: Array<any>, x: number, down: boolean) {
        const newArr = arr;
        if (down) {
            if (x === 0) {
                return newArr;
            }
            const temp = newArr[x];
            newArr[x] = newArr[x - 1];
            newArr[x - 1] = temp;
            return newArr;
        }
        // if up
        if (x === newArr.length - 1) {
            return arr;
        }
        const temp = newArr[x];
        newArr[x] = newArr[x + 1];
        newArr[x + 1] = temp;
        return newArr;
    }

    const shiftDownwards = () => {
        setModifiedForm(true);
        const selected = { ...allrows };
        let currArray = [...data];
        for (let i = 0; i < selected.selected.length; i++) {
            currArray = swap(currArray, selected.selected[i], false);
            if (selected.selected[i] !== currArray.length - 1) selected.selected[i] += 1;
        }
        setData(currArray);
        updateSelected(selected);
        // console.log(data);
        console.log(allrows.selected);
    };

    const shiftUpwards = () => {
        setModifiedForm(true);
        const selected = { ...allrows };
        let currArray = [...data];
        for (let i = 0; i < selected.selected.length; i++) {
            currArray = swap(currArray, selected.selected[i], true);
            if (selected.selected[i] !== 0) selected.selected[i] -= 1;
        }
        setData(currArray);
        updateSelected(selected);
        // console.log(data);
        console.log(allrows.selected);
    };
    const addRow = () => {
        setModifiedForm(true);
        const newData = {
            fieldName: 'new Field',
            key: data.length + 1,
            datahubType: 'STRING',
            nativeDataType: 'freetext - users can view this description when they mouse over Data Type in Schema tab',
            fieldDescription: '',
            fieldTags: [''],
            fieldGlossaryTerms: [''],
            editKey: (data.length + 1).toString(),
        };
        const newArr = [...data];
        newArr[data.length] = newData;
        setData(newArr);
    };
    const resetState = () => {
        setData(formalData);
        allrows.selected = [] as any;
        updateSelected(allrows);
        setModifiedForm(false);
    };
    console.log('all rows loaded');
    return (
        <Form form={form} component={false}>
            <Row>
                <Button onClick={addRow}>Add New Row</Button>
                <Button onClick={deleteRow}>Delete Row</Button>
                &nbsp;
                <Button onClick={shiftUpwards}>&#x2191;</Button>
                <Button onClick={shiftDownwards}>&#x2193;</Button>
                &nbsp;
                <Button onClick={submitData} disabled={!modifiedForm}>
                    Submit Changes
                </Button>
                <Button onClick={resetState} disabled={!modifiedForm}>
                    Reset Changes
                </Button>
            </Row>
            <Divider dashed orientation="left">
                Refresh webpage with <b>F5</b> after successful submission to see updated schema.
            </Divider>
            <Table
                components={{
                    body: {
                        cell: EditableCell,
                    },
                }}
                rowSelection={{
                    type: 'checkbox',
                    ...rowSelection,
                }}
                bordered
                dataSource={data}
                columns={mergedColumns}
                rowClassName="editable-row"
                pagination={false}
            />
        </Form>
    );
};
