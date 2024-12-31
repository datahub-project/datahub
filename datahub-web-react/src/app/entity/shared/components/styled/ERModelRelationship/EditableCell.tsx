import React, { useContext } from 'react';
import { Form, Select } from 'antd';
import { EditableContext } from './ERModelRelationUtils';
import { Dataset } from '../../../../../../types.generated';

interface EditableCellProps {
    editable: boolean;
    children: React.ReactNode;
    dataIndex: keyof ERModelRelationRecord;
    record: ERModelRelationRecord;
    tableRecord?: Dataset;
    value?: any;
    handleSave: (record: ERModelRelationRecord) => void;
}
interface ERModelRelationRecord {
    key: string;
    field1Name: string;
    field2Name: string;
}
export const EditableCell = ({
    editable,
    children,
    dataIndex,
    record,
    tableRecord,
    value,
    handleSave,
    ...restProps
}: EditableCellProps) => {
    const form = useContext(EditableContext)!;
    const save = async () => {
        try {
            const values = await form.validateFields();
            handleSave({ ...record, ...values });
        } catch (errInfo) {
            console.log('Save failed:', errInfo);
        }
    };

    let childNode = children;
    if (editable) {
        childNode = (
            <Form.Item
                style={{ margin: 0 }}
                name={dataIndex}
                rules={[
                    {
                        required: true,
                        message: `Field is required.`,
                    },
                ]}
            >
                <Select
                    size="large"
                    className="ermodelrelation-select-selector"
                    options={tableRecord?.schemaMetadata?.fields?.map((result) => ({
                        value: result.fieldPath,
                        label: result.fieldPath,
                    }))}
                    value={value}
                    disabled={tableRecord?.schemaMetadata?.fields?.length === 0}
                    onChange={save}
                    placeholder="Select a field"
                />
            </Form.Item>
        );
        if (record[dataIndex] !== '') {
            form.setFieldsValue({ [dataIndex]: record[dataIndex] });
        }
    } else {
        childNode = <div className="editable-cell-value-wrap">{children}</div>;
    }

    return <td {...restProps}>{childNode}</td>;
};
