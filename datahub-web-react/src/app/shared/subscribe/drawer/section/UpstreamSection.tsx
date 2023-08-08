import React from 'react';
import { Link } from 'react-router-dom';
import { Switch, Typography } from 'antd';
import styled from 'styled-components/macro';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { getEntityPath } from '../../../../entity/shared/containers/profile/utils';
import { EntityType } from '../../../../../types.generated';
import { ReactComponent as LinkOut } from '../../../../../images/link-out.svg';
import { useDrawerState } from '../state/context';
import useDrawerActions from '../state/actions';

const UpstreamContainer = styled.div`
    margin-top: 32px;
    margin-left: 8px;
    display: grid;
    grid-template-columns: 1fr 15fr;
    column-gap: 8px;
    align-items: center;
`;

const UpstreamSwitch = styled(Switch)`
    grid-column: 1;
    grid-row: 1;
`;

const TitleText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 16px;
    line-height: 24px;
    font-weight: 700;
    grid-column: 2;
    grid-row: 1;
`;

const SubtitleText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    line-height: 20px;
    font-weight: 500;
    grid-column: 2;
    grid-row: 2;
`;

interface Props {
    entityUrn: string;
    entityType: EntityType;
    upstreamCount: number;
}

export default function UpstreamSection({ entityUrn, entityType, upstreamCount }: Props) {
    const { subscribeToUpstream } = useDrawerState();
    const actions = useDrawerActions();
    const entityRegistry = useEntityRegistry();
    // TODO: The filter degree may need to be more than 1 in the future
    const upstreamLineageTabPath: string = getEntityPath(
        entityType,
        entityUrn,
        entityRegistry,
        false,
        false,
        'Lineage',
        {
            filter_degree___false___EQUAL___0: '1',
        },
    );

    return (
        <>
            <UpstreamContainer>
                <UpstreamSwitch
                    size="small"
                    checked={subscribeToUpstream}
                    onChange={(checked) => actions.setSubscribeToUpstream(checked)}
                />
                <TitleText>Subscribe to changes for all upstream entities</TitleText>
                <SubtitleText>
                    There are currently{' '}
                    <Link to={upstreamLineageTabPath} target="_blank" rel="noopener noreferrer">
                        {`${upstreamCount} upstream entities.`}
                        <LinkOut style={{ marginLeft: '4px' }} />
                    </Link>
                </SubtitleText>
            </UpstreamContainer>
        </>
    );
}
