import { Tag } from 'antd';
import React from 'react';
import { BookOutlined } from '@ant-design/icons';

import { GlossaryTerms } from '../../../types.generated';

type Props = {
    glossaryTerms: GlossaryTerms | null;
};

export default function GlossaryTermGroup({ glossaryTerms }: Props) {
    return (
        <>
            {glossaryTerms?.terms?.map((term) => (
                <Tag color="blue" closable={false}>
                    {term.term.name}
                    <BookOutlined style={{ marginLeft: '2%' }} />
                </Tag>
            ))}
        </>
    );
}
