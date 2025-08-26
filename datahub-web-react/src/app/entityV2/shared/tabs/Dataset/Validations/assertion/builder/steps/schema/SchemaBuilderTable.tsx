import { PlusOutlined } from '@ant-design/icons';
import { Button, Tooltip } from '@components';
import { DeleteOutline } from '@mui/icons-material';
import { Input, Select, Table } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { EditButton } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/details/EditButton';
import { SaveButton } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/details/SaveButton';
import {
    areExpectedColumnsValid,
    supportedSchemaFieldTypes,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/schema/utils';

import { SchemaAssertionField, SchemaFieldDataType } from '@types';

const ButtonWrapper = styled.div`
    max-width: 100px;
`;

const StyledPlusOutlined = styled(PlusOutlined)`
    && {
        font-size: 12px;
    }
`;

const StyledTable = styled(Table)`
    max-width: 100px;
    overflow: none;
`;

const Name = styled.div`
    max-width: 280px;
`;

const Path = styled.div`
    overflow: ellipsis;
`;

type Props = {
    selected: Partial<SchemaAssertionField>[];
    onChange: (newFields: SchemaAssertionField[]) => void;
    disabled?: boolean;
    options?: SchemaAssertionField[];
};

/**
 * Schema builder table
 */
export const SchemaBuilderTable = ({ selected, onChange, disabled, options }: Props) => {
    const initialData = selected.map((field, i) => ({
        key: i,
        path: field.path,
        type: field.type,
        nativeType: field.nativeType,
    }));
    const [tableData, setTableData] = useState(initialData);
    const [index, setIndex] = useState(selected.length);
    const [editing, setEditing] = useState<boolean>(false);

    // Only allow saving the expected columns if there are not duplicates
    // of the same column with differing types.
    const validExpectedColumns = areExpectedColumnsValid(tableData);

    useEffect(() => {
        const newTableData = selected.map((field, i) => ({
            key: i,
            path: field.path,
            type: field.type,
            nativeType: field.nativeType,
        }));
        setIndex(selected.length);
        setTableData(newTableData);
    }, [selected]);

    const handleAdd = () => {
        const newData = {
            key: index,
            path: '',
            type: SchemaFieldDataType.String,
            nativeType: null,
        };
        setTableData([...tableData, newData]);
        setIndex(index + 1);
    };

    const handleRemove = (key) => {
        const newData = tableData.filter((item) => item.key !== key);
        setTableData(newData);
    };

    const handleRemoveAll = () => {
        setTableData([]);
    };

    const handleFieldChange = (value, key, dataIndex) => {
        const newData = [...tableData];
        const i = newData.findIndex((item) => key === item.key);
        if (i > -1) {
            newData[i][dataIndex] = value;
            setTableData(newData);
        }
    };

    const selectFieldOption = (path, key) => {
        const newData = [...tableData];
        const option = options?.find((o) => o.path === path);
        const i = newData.findIndex((item) => key === item.key);
        if (i > -1 && option) {
            newData[i] = {
                key,
                path: option.path,
                type: option.type,
                nativeType: option.nativeType,
            };
            setTableData(newData);
        }
    };

    const onDoneEditing = () => {
        onChange(
            tableData
                .filter((row) => row.path && row.type)
                .map((row) => ({
                    path: row.path as string,
                    type: row.type as SchemaFieldDataType,
                    nativeType: row.nativeType,
                })),
        );
        setEditing(false);
    };

    // TODO: Refactor into broken down components.
    const columns = [
        {
            title: 'Name',
            dataIndex: 'path',
            key: 'path',
            render: (text, record) => (
                <Name key={record.key}>
                    {editing ? (
                        (!options && (
                            <Input
                                disabled={disabled}
                                value={text}
                                onChange={(e) => handleFieldChange(e.target.value, record.key, 'path')}
                            />
                        )) || (
                            <Select
                                disabled={disabled}
                                value={text}
                                style={{ maxWidth: 260 }}
                                dropdownMatchSelectWidth={false}
                                onSelect={(value) => selectFieldOption(value, record.key)}
                            >
                                {options?.map((option) => (
                                    <Select.Option key={option.path} value={option.path}>
                                        {option.path}
                                    </Select.Option>
                                ))}
                            </Select>
                        )
                    ) : (
                        <Tooltip showArrow={false} title={text}>
                            <Path>{text}</Path>
                        </Tooltip>
                    )}
                </Name>
            ),
        },
        {
            title: 'Type',
            dataIndex: 'type',
            key: 'type',
            render: (text, record) => (
                <Select
                    key={record.key}
                    disabled={disabled || !editing}
                    value={text}
                    onChange={(value) => handleFieldChange(value, record.key, 'type')}
                    style={{ width: 120 }}
                >
                    {supportedSchemaFieldTypes.map((type) => (
                        <Select.Option key={type.type} value={type.type}>
                            {type.name}
                        </Select.Option>
                    ))}
                </Select>
            ),
        },
        {
            title: (
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                    <Tooltip title="Remove all columns" showArrow={false} placement="left">
                        <Button
                            style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', border: 'none' }}
                            disabled={disabled || !editing}
                            onClick={() => handleRemoveAll()}
                        >
                            <DeleteOutline style={{ fontSize: 16 }} />
                        </Button>
                    </Tooltip>
                </div>
            ),
            key: 'action',
            render: (record) => (
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                    <Tooltip title="Remove column from expectation set" showArrow={false} placement="left">
                        <Button
                            style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', border: 'none' }}
                            disabled={disabled || !editing}
                            onClick={() => handleRemove(record.key)}
                        >
                            <DeleteOutline style={{ fontSize: 16 }} />
                        </Button>
                    </Tooltip>
                </div>
            ),
        },
    ];

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
            <ButtonWrapper>
                {(!editing && (
                    <EditButton
                        title="Change"
                        disabled={disabled}
                        tooltip="Edit expected columns"
                        onClick={() => {
                            if (!disabled) setEditing(true);
                        }}
                    />
                )) || (
                    <SaveButton
                        disabled={!validExpectedColumns}
                        title="Done"
                        tooltip={
                            !validExpectedColumns
                                ? 'Invalid expectations found. Please check expected columns for duplicate or incomplete entries!'
                                : 'Confirm expected columns'
                        }
                        onClick={onDoneEditing}
                    />
                )}
            </ButtonWrapper>
            <StyledTable
                columns={columns}
                dataSource={tableData}
                pagination={false}
                rowKey="key"
                bordered
                locale={{
                    emptyText: 'No expected columns',
                }}
            />
            {editing ? (
                <Button onClick={handleAdd} style={{ marginBottom: 16 }}>
                    <StyledPlusOutlined /> Add Column
                </Button>
            ) : null}
        </div>
    );
};
