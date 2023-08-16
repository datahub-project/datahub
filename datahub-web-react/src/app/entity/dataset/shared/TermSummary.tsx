import React from 'react';
import { Tag } from 'antd';
import { BookOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { useGetGlossaryTermQuery } from '../../../../graphql/glossaryTerm.generated';
import { HoverEntityTooltip } from '../../../recommendations/renderer/component/HoverEntityTooltip';
import { EntityType, GlossaryTerm } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';

const TermLink = styled.span`
    display: inline-block;
`;

type Props = {
    urn: string;
    mode?: 'text' | 'default';
};

export const TermSummary = ({ urn, mode }: Props) => {
    const entityRegistry = useEntityRegistry();
    const { data } = useGetGlossaryTermQuery({ variables: { urn } });
    const termName = data ? entityRegistry.getDisplayName(EntityType.GlossaryTerm, data?.glossaryTerm) : null;

    if (mode === 'text') return <>{termName}</>;

    return (
        <>
            {data && (
                <HoverEntityTooltip entity={data?.glossaryTerm as GlossaryTerm}>
                    <TermLink key={data?.glossaryTerm?.urn}>
                        <Tag closable={false} style={{ cursor: 'pointer' }}>
                            <BookOutlined style={{ marginRight: '3%' }} />
                            {termName}
                        </Tag>
                    </TermLink>
                </HoverEntityTooltip>
            )}
        </>
    );
};
