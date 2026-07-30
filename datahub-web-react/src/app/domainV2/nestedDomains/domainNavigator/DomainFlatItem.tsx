import { Tooltip } from '@components';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { useDomainsContext as useDomainsContextV2 } from '@app/domainV2/DomainsContext';
import { DeprecationIcon } from '@app/entityV2/shared/components/styled/DeprecationIcon';
import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { ListDomainFragment } from '@graphql/domain.generated';
import { Domain } from '@types';

// Top-level row chrome mirrors the tree row in `DomainNode` (38px height,
// 6px radius, hover/selected states, brand-gradient title when selected) so
// the flat-list mode reads as a continuation of the same surface — only the
// content layout differs (no caret, no level indent, parent path below).
const RowContainer = styled.div<{ $isSelected: boolean }>`
    position: relative;
    display: flex;
    align-items: center;
    padding: 4px 8px;
    min-height: 38px;
    cursor: pointer;
    border-radius: 6px;
    transition: background-color 0.15s ease;
    margin: 0 2px 2px 2px;

    ${(props) =>
        props.$isSelected &&
        `
        background: ${props.theme.colors.bgSelectedSubtle};
        box-shadow: ${props.theme.colors.shadowFocusBrand};
    `}

    ${(props) =>
        !props.$isSelected &&
        `
        &:hover {
            background: ${props.theme.colors.bgHover};
            box-shadow: ${props.theme.colors.shadowFocus};
        }
    `}
`;

const IconSlot = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 24px;
    height: 20px;
    margin-right: 8px;
    flex-shrink: 0;
`;

// Stacked content — name on top, breadcrumb below. min-width:0 lets the
// children's overflow-ellipsis actually kick in instead of growing the row.
const TextStack = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    min-width: 0;
    overflow: hidden;
`;

const TitleRow = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
    min-width: 0;
    overflow: hidden;
`;

const DeprecationSlot = styled.span`
    display: inline-flex;
    align-items: center;
    flex-shrink: 0;
    line-height: 0;

    & svg {
        width: 12px;
        height: 12px;
    }
`;

const Title = styled.span<{ $isSelected: boolean }>`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-size: 14px;
    line-height: 20px;
    color: ${(props) => props.theme.colors.textSecondary};

    ${(props) =>
        props.$isSelected &&
        `
        background: ${props.theme.colors.brandGradientSelected};
        background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 600;
    `}
`;

const Breadcrumb = styled.span`
    display: flex;
    align-items: center;
    overflow: hidden;
    font-size: 11px;
    line-height: 16px;
    color: ${(props) => props.theme.colors.textTertiary};
`;

const BreadcrumbSegment = styled.span`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    /* Allow each segment to shrink — without this, deep paths overflow the
       row instead of getting ellipsis. flex:0 1 auto means "preferred width
       is content, can shrink, won't grow". */
    flex: 0 1 auto;
    min-width: 0;
`;

const BreadcrumbSeparator = styled(CaretRight)`
    flex-shrink: 0;
    margin: 0 2px;
`;

interface Props {
    domain: ListDomainFragment;
}

/**
 * Single row in the sidebar's flat-list mode. Mounted by `DomainNavigator`
 * when an owner filter is active, instead of the recursive `DomainNode` tree.
 *
 * Renders the domain icon + name, with the ancestor path beneath. Clicking
 * navigates straight to the domain's entity page — no expand affordance,
 * because the row has no children to expand to.
 */
export default function DomainFlatItem({ domain }: Props) {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const { entityData } = useDomainsContextV2();

    const isOnEntityPage = !!entityData && entityData.urn === domain.urn;
    const displayName = entityRegistry.getDisplayName(domain.type, isOnEntityPage ? entityData : domain);
    const deprecation = isOnEntityPage ? entityData?.deprecation : domain.deprecation;

    // `parentDomains.domains` is returned by GMS in leaf→root order (the
    // immediate parent first). Reverse to display as root→leaf, which is
    // how breadcrumbs are conventionally read.
    const ancestors = [...(domain.parentDomains?.domains ?? [])].reverse();
    const ancestorNames = ancestors
        .map((a) => entityRegistry.getDisplayName(a.type, a as unknown as Domain))
        .filter(Boolean);

    const handleClick = () => {
        history.push(entityRegistry.getEntityUrl(domain.type, domain.urn));
    };

    return (
        <RowContainer $isSelected={isOnEntityPage} onClick={handleClick} data-testid={`domain-flat-item-${domain.urn}`}>
            <IconSlot>
                <DomainColoredIcon domain={domain as Domain} size={20} fontSize={12} />
            </IconSlot>
            <TextStack>
                <TitleRow>
                    <Tooltip placement="right" title={displayName} mouseEnterDelay={0.7} mouseLeaveDelay={0}>
                        <Title $isSelected={isOnEntityPage}>{displayName}</Title>
                    </Tooltip>
                    {deprecation?.deprecated && (
                        <DeprecationSlot>
                            <DeprecationIcon
                                urn={domain.urn}
                                deprecation={deprecation}
                                showUndeprecate={false}
                                showText={false}
                            />
                        </DeprecationSlot>
                    )}
                </TitleRow>
                {ancestorNames.length > 0 && (
                    <Tooltip
                        placement="bottom"
                        title={ancestorNames.join(' / ')}
                        mouseEnterDelay={0.7}
                        mouseLeaveDelay={0}
                    >
                        <Breadcrumb>
                            {ancestorNames.map((name, idx) => (
                                // Index keys are fine here — the breadcrumb
                                // is regenerated wholesale on every render
                                // and the segments aren't reorderable.
                                // eslint-disable-next-line react/no-array-index-key
                                <React.Fragment key={`${domain.urn}-crumb-${idx}`}>
                                    <BreadcrumbSegment>{name}</BreadcrumbSegment>
                                    {idx < ancestorNames.length - 1 && <BreadcrumbSeparator size={10} weight="bold" />}
                                </React.Fragment>
                            ))}
                        </Breadcrumb>
                    </Tooltip>
                )}
            </TextStack>
        </RowContainer>
    );
}
