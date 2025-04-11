import { EntityActionType, EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import LaunchIcon from '@mui/icons-material/Launch';
import { GenericEntityProperties } from '@src/app/entity/shared/types';
import React from 'react';
import styled from 'styled-components';
import { getSiblings } from '../tabs/Dataset/Validations/acrylUtils';
import { getExternalUrlDisplayName } from '../utils';

const Link = styled.a`
    display: flex;
    align-items: center;
    gap: 5px;

    border-radius: 4px;
    padding: 4px 6px;
    margin: 0px 4px;

    background: ${REDESIGN_COLORS.LIGHT_TEXT_DARK_BACKGROUND};
    color: ${REDESIGN_COLORS.TITLE_PURPLE};
`;

const IconWrapper = styled.span`
    display: flex;
    align-items: center;
    font-size: ${4 / 3}em;
`;

const Links = styled.div`
    display: flex;
    justify-content: end;
    gap: 4px;
    flex-wrap: wrap;
`;

const MAX_VISIBILE_ACTIONS = 2;

interface Props {
    data: GenericEntityProperties | null;
    className?: string;
    hideSiblingActions?: boolean;
    urn: string;
}

export default function ViewInPlatform({ urn, className, data, hideSiblingActions }: Props) {
    const separateSiblings = useIsSeparateSiblingsMode();
    if (!data) return null;

    function sendAnalytics() {
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.ClickExternalUrl,
            entityUrn: urn,
            entityType: data?.type ?? undefined,
        });
    }
    const externalUrl = data?.properties?.externalUrl;
    const parentPlatformName = getExternalUrlDisplayName(data);
    const defaultAction = externalUrl ? [{ displayName: parentPlatformName || 'source', url: externalUrl }] : [];

    let visibleActions: any = [...defaultAction];
    if (!(hideSiblingActions ?? separateSiblings)) {
        const siblings = getSiblings(data);
        if (siblings && siblings.length) {
            const siblingActions: any = siblings
                .map((sibling) => {
                    if (sibling?.platform?.name && sibling?.properties?.externalUrl) {
                        return {
                            displayName: getExternalUrlDisplayName(sibling),
                            url: sibling.properties.externalUrl,
                        };
                    }
                    return null;
                })
                .filter((action) => action !== null);

            visibleActions = [...defaultAction, ...siblingActions].slice(0, MAX_VISIBILE_ACTIONS);
        }
    }

    if (!visibleActions.length) return null;

    return (
        <Links>
            {visibleActions.map((action) => (
                <div>
                    <Link href={action.url} target="_blank" onClick={sendAnalytics} className={className}>
                        <IconWrapper>
                            <LaunchIcon fontSize="inherit" />
                        </IconWrapper>
                        View in {action.displayName}
                    </Link>
                </div>
            ))}
        </Links>
    );
}
