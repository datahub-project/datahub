import React from 'react';
import { Tag } from 'antd';
import { BookOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { useGetGlossaryTermQuery } from '../../../../graphql/glossaryTerm.generated';
import { HoverEntityTooltip } from '../../../recommendations/renderer/component/HoverEntityTooltip';

const TermLink = styled.span`
    display: inline-block;
`;

type Props = {
    urn: string;
};

export const TermSummary = ({ urn }: Props) => {
    const { data } = useGetGlossaryTermQuery({ variables: { urn } });

    return (
        <HoverEntityTooltip>
            <TermLink key={data?.glossaryTerm?.urn}>
                <Tag closable={false} style={{ cursor: 'pointer' }}>
                    <BookOutlined style={{ marginRight: '3%' }} />
                    {data?.glossaryTerm?.name}
                </Tag>
            </TermLink>
        </HoverEntityTooltip>
    );
};
