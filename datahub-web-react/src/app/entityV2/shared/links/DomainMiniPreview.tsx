import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { Domain, EntityType } from '../../../../types.generated';
import { DomainColoredIcon } from './DomainColoredIcon';
import EntityCount from '../containers/profile/header/EntityCount';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { HoverEntityTooltip } from '../../../recommendations/renderer/component/HoverEntityTooltip';
import { ANTD_GRAY } from '../../../entity/shared/constants';

const DomainLinkContainer = styled.div`
    display: flex;
    flex-direction: row;
    :hover {
        background-color: #f5f7fa;
    }
    border-radius: 12px;
    cursor: pointer;
    padding: 4px;
`;

const DomainInfoContainer = styled.div`
    display: flex;
    flex-direction: column;
    margin-left: 8px;
`;

const DomainTitle = styled.div`
    font-size: 12px;
    font-weight: 400;
    color: ${ANTD_GRAY[9]};
    font-family: Mulish;
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
    max-width: 200px;
`;

const DomainContents = styled.div`
    font-size: 12px;
    font-weight: 400;
    color: ${ANTD_GRAY[7]};
    font-family: Mulish;
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
`;

export const DomainMiniPreview = ({ domain }: { domain: Domain }): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const url = entityRegistry.getEntityUrl(EntityType.Domain, domain.urn as string);

    return (
        <Link to={url}>
            <HoverEntityTooltip entity={domain} placement="bottom" showArrow={false}>
                <DomainLinkContainer>
                    <DomainColoredIcon domain={domain} />
                    <DomainInfoContainer>
                        <DomainTitle>{domain?.properties?.name}</DomainTitle>
                        <DomainContents>
                            <EntityCount displayAssetsText entityCount={domain?.entities?.total} />
                        </DomainContents>
                    </DomainInfoContainer>
                </DomainLinkContainer>
            </HoverEntityTooltip>
        </Link>
    );
};
