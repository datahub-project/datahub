import { DatabaseOutlined, FileDoneOutlined } from '@ant-design/icons';
import { VerticalDivider } from '@remirror/react';
import React from 'react';
import styled from 'styled-components';

import DomainIcon from '@app/domain/DomainIcon';
import { ANTD_GRAY_V2 } from '@app/entity/shared/constants';
import { pluralize } from '@app/shared/textUtil';
import { useHoverEntityTooltipContext } from '@src/app/recommendations/HoverEntityTooltipContext';

import { SearchResultFields_Domain_Fragment } from '@graphql/search.generated';

const Wrapper = styled.div`
    color: ${ANTD_GRAY_V2[8]};
    font-size: 12px;
    display: flex;
    align-items: center;
    line-height: 20px;

    svg {
        margin-right: 4px;
    }
`;

const StyledDivider = styled(VerticalDivider)`
    &&& {
        margin: 0 8px;
    }
`;

interface Props {
    domain: SearchResultFields_Domain_Fragment;
}

export default function DomainEntitiesSnippet({ domain }: Props) {
    const { entityCount } = useHoverEntityTooltipContext();
    const subDomainCount = domain.children?.total || 0;
    const dataProductCount = domain.dataProducts?.total || 0;

    return (
        <Wrapper>
            {!!entityCount && (
                <>
                    <DatabaseOutlined /> {entityCount} {entityCount === 1 ? 'entity' : 'entities'}
                    <StyledDivider />
                </>
            )}
            <DomainIcon /> {subDomainCount} {pluralize(subDomainCount, 'sub-domain')}
            <StyledDivider />
            <FileDoneOutlined /> {dataProductCount} {pluralize(dataProductCount, 'data product')}
        </Wrapper>
    );
}
