/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Form } from 'antd';
import React from 'react';

import { EditableContext } from '@app/entity/shared/components/styled/ERModelRelationship/ERModelRelationUtils';

interface EditableRowProps {
    index: number;
}
export const EditableRow: React.FC<EditableRowProps> = ({ ...propsAt }) => {
    const [form] = Form.useForm();
    return (
        <Form form={form} component={false}>
            <EditableContext.Provider value={form}>
                <tr {...propsAt} />
            </EditableContext.Provider>
        </Form>
    );
};
