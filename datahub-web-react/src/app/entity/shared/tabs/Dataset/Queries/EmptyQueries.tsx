/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { PlusOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';

import { EmptyTab } from '@app/entity/shared/components/styled/EmptyTab';

export type Props = {
    message?: string;
    readOnly?: boolean;
    onClickAddQuery: () => void;
};

export default function EmptyQueries({ message, readOnly = false, onClickAddQuery }: Props) {
    return (
        <EmptyTab tab="queries">
            {!readOnly && !message && (
                <Button onClick={onClickAddQuery}>
                    <PlusOutlined /> Add Query
                </Button>
            )}
        </EmptyTab>
    );
}
