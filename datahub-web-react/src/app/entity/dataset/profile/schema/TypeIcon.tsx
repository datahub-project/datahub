import {
    FieldBinaryOutlined,
    NumberOutlined,
    UnorderedListOutlined,
    CloseCircleOutlined,
    UnderlineOutlined,
} from '@ant-design/icons';
import { Typography } from 'antd';
import React, { FC } from 'react';
import { VscSymbolString, VscFileBinary } from 'react-icons/vsc';
import styled from 'styled-components';
import { SchemaFieldDataType } from '../../../../../types.generated';

const TypeSubtitle = styled.div`
    font-size: 8px;
`;

const DATA_TYPE_ICON_MAP: Record<
    SchemaFieldDataType,
    { icon: FC<{ style: any }> | null; size: number; text: string }
> = {
    [SchemaFieldDataType.Boolean]: {
        icon: FieldBinaryOutlined,
        size: 18,
        text: 'Boolean',
    },
    [SchemaFieldDataType.Fixed]: { icon: FieldBinaryOutlined, size: 18, text: 'Fixed' },
    [SchemaFieldDataType.String]: { icon: VscSymbolString, size: 20, text: 'String' },
    [SchemaFieldDataType.Bytes]: {
        icon: VscFileBinary,
        size: 18,
        text: 'Bytes',
    },
    [SchemaFieldDataType.Number]: { icon: NumberOutlined, size: 14, text: 'Number' },
    [SchemaFieldDataType.Enum]: { icon: UnorderedListOutlined, size: 18, text: 'Enum' },
    [SchemaFieldDataType.Null]: { icon: CloseCircleOutlined, size: 14, text: 'Null' },
    [SchemaFieldDataType.Map]: { icon: null, size: 0, text: 'Map' },
    [SchemaFieldDataType.Array]: { icon: UnorderedListOutlined, size: 14, text: 'Array' },
    [SchemaFieldDataType.Union]: { icon: UnderlineOutlined, size: 14, text: 'Union' },
    [SchemaFieldDataType.Struct]: { icon: null, size: 0, text: 'Struct' },
};

type Props = {
    type: SchemaFieldDataType;
};

export default function TypeIcon({ type }: Props) {
    const { icon: Icon, size, text } = DATA_TYPE_ICON_MAP[type];
    return (
        <span data-testid={`icon-${type}`}>
            {Icon && <Icon style={{ fontSize: size }} />}
            <TypeSubtitle>
                <Typography.Text type="secondary">{text}</Typography.Text>
            </TypeSubtitle>
        </span>
    );
}
