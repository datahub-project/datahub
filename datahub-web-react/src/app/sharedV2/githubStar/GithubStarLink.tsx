import { Icon } from '@components';
import { Star } from '@phosphor-icons/react/dist/csr/Star';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import analytics, { EventType, GithubStarClickEvent } from '@app/analytics';
import { DATAHUB_GITHUB_URL } from '@app/sharedV2/constants';
import { formatNumber } from '@app/sharedV2/formatNumber';

const SplitButton = styled.a`
    display: inline-flex;
    align-items: stretch;
    height: 28px;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 6px;
    overflow: hidden;
    font-size: 12px;
    line-height: 1;
    color: ${(props) => props.theme.colors.text};
    text-decoration: none;

    &:hover {
        color: ${(props) => props.theme.colors.text};
    }
`;

const Segment = styled.span`
    display: inline-flex;
    align-items: center;
    padding: 0 10px;
`;

const StarSegment = styled(Segment)`
    gap: 4px;
    font-weight: 700;
    background-color: ${(props) => props.theme.colors.bg};

    ${SplitButton}:hover & {
        background-color: ${(props) => props.theme.colors.bgHover};
    }
`;

const CountSegment = styled(Segment)`
    font-weight: 600;
    background-color: ${(props) => props.theme.colors.bgSurface};
    border-left: 1px solid ${(props) => props.theme.colors.border};
`;

type Props = {
    starCount: number | null;
    source: GithubStarClickEvent['source'];
    className?: string;
    dataTestId?: string;
};

export default function GithubStarLink({ starCount, source, className, dataTestId = 'github-star-button' }: Props) {
    const { t } = useTranslation('shared.misc');

    const handleClick = () => analytics.event({ type: EventType.GithubStarClickEvent, source });

    return (
        <SplitButton
            className={className}
            href={DATAHUB_GITHUB_URL}
            target="_blank"
            rel="noopener noreferrer"
            aria-label={t('githubStar.ariaLabel')}
            onClick={handleClick}
            data-testid={dataTestId}
        >
            <StarSegment>
                <Icon icon={Star} weight="fill" size="md" color="tagsTrueYellowIcon" />
                {t('githubStar.label')}
            </StarSegment>
            {starCount !== null && <CountSegment>{formatNumber(starCount)}</CountSegment>}
        </SplitButton>
    );
}
