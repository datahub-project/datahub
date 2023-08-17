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
};

export const TermSummary = ({ urn }: Props) => {
    const entityRegistry = useEntityRegistry();
    const { data } = useGetGlossaryTermQuery({ variables: { urn } });

    return (
        <>
            {data && (
                <HoverEntityTooltip entity={data?.glossaryTerm as GlossaryTerm}>
                    <TermLink key={data?.glossaryTerm?.urn}>
                        <Tag closable={false} style={{ cursor: 'pointer' }}>
                            <BookOutlined style={{ marginRight: '3%' }} />
                            {entityRegistry.getDisplayName(EntityType.GlossaryTerm, data?.glossaryTerm)}
                        </Tag>
                    </TermLink>
                </HoverEntityTooltip>
            )}
        </>
    );
};
