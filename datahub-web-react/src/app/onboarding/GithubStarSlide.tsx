import { Icon, Pill } from '@components';
import { SidebarSimple } from '@phosphor-icons/react/dist/csr/SidebarSimple';
import { StarFour } from '@phosphor-icons/react/dist/csr/StarFour';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled, { DefaultTheme } from 'styled-components';

import { SlideContainer, SlideDescription, SlideTitle } from '@app/onboarding/WelcomeToDataHubModal.components';
import { formatNumber } from '@app/sharedV2/formatNumber';
import GithubStarLink from '@app/sharedV2/githubStar/GithubStarLink';
import useGithubStarCount from '@app/sharedV2/githubStar/useGithubStarCount';

const REPO_OWNER = 'datahub-project';
const REPO_NAME = 'datahub';
const REPO_TOPICS = ['metadata', 'data-catalog', 'context', 'ai-agents'];
type ColorToken = keyof DefaultTheme['colors'];

const WINDOW_DOT_COLORS: ColorToken[] = ['chartsWineMedium', 'chartsYellowMedium', 'chartsOliveMedium'];
const AVATAR_COLORS: ColorToken[] = [
    'glossaryPalettePeach',
    'tagsTrueBlueBorder',
    'chartsOliveMedium',
    'tagsLavenderBorder',
    'chartsWineMedium',
    'chartsSeafoamMedium',
];

const Illustration = styled.div`
    position: relative;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 20px;
    min-height: 340px;
    padding-top: 24px;
`;

const BrowserWindow = styled.div`
    position: relative;
    display: flex;
    justify-content: center;
    width: 546px;
    padding: 44px 0 16px;
    border-radius: 16px;
    background: radial-gradient(ellipse at center, ${(props) => props.theme.colors.bgSurfaceBrand} 0%, transparent 70%);
`;

const WindowDots = styled.div`
    position: absolute;
    top: 12px;
    left: 20px;
    display: flex;
    gap: 3px;
`;

const WindowDot = styled.span<{ $color: ColorToken }>`
    width: 9px;
    height: 9px;
    border-radius: 50%;
    background-color: ${(props) => props.theme.colors[props.$color]};
`;

const RepoCard = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
    width: 404px;
    padding: 20px;
    border-radius: 12px;
    background-color: ${(props) => props.theme.colors.bg};
    box-shadow: ${(props) => props.theme.colors.shadowLg}, ${(props) => props.theme.colors.shadowXl};
`;

const RepoHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
`;

const RepoName = styled.span`
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 16px;
    color: ${(props) => props.theme.colors.textSecondary};

    strong {
        color: ${(props) => props.theme.colors.text};
    }
`;

const StarHighlight = styled.span`
    position: relative;
    display: inline-flex;
    padding: 2px;
    border-radius: 11px;
    background: linear-gradient(
        200deg,
        ${(props) => props.theme.colors.chartsInformationLow} 0%,
        ${(props) => props.theme.colors.colorPickerCobalt} 40%,
        ${(props) => props.theme.colors.borderHover} 100%
    );
`;

const StarHighlightInner = styled.span`
    display: inline-flex;
    padding: 3px;
    border-radius: 9px;
    background-color: ${(props) => props.theme.colors.bg};
`;

const SparkleMark = styled.span<{ $top: number; $left?: number; $right?: number; $size: number; $color: ColorToken }>`
    position: absolute;
    top: ${(props) => props.$top}px;
    ${(props) => (props.$left !== undefined ? `left: ${props.$left}px;` : `right: ${props.$right}px;`)}
    display: flex;
    font-size: ${(props) => props.$size}px;
    color: ${(props) => props.theme.colors[props.$color]};
`;

const RepoDescription = styled.span`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const Topics = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
`;

const Stargazers = styled.div`
    display: flex;
    align-self: stretch;
    padding: 0 16px 0 calc((100% - 404px) / 2 + 40px);
    align-items: center;
    gap: 8px;
    font-size: 14px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const AvatarStack = styled.div`
    display: flex;
    flex-shrink: 0;
`;

const AvatarDot = styled.span<{ $color: ColorToken }>`
    width: 24px;
    height: 24px;
    border-radius: 50%;
    box-shadow: 0 0 0 1.5px ${(props) => props.theme.colors.bg};
    background-color: ${(props) => props.theme.colors[props.$color]};

    & + & {
        margin-left: -4px;
    }
`;

export default function GithubStarSlide() {
    const { t } = useTranslation('onboarding');
    const starCount = useGithubStarCount(true);

    return (
        <SlideContainer>
            <SlideTitle>{t('welcome.slideGithubTitle')}</SlideTitle>
            <SlideDescription>{t('welcome.slideGithubDescription')}</SlideDescription>
            <Illustration>
                <BrowserWindow>
                    <WindowDots aria-hidden>
                        {WINDOW_DOT_COLORS.map((color) => (
                            <WindowDot key={color} $color={color} />
                        ))}
                    </WindowDots>
                    <RepoCard>
                        <RepoHeader>
                            <RepoName>
                                <Icon icon={SidebarSimple} weight="duotone" size="xl" color="icon" />
                                <span>
                                    {REPO_OWNER} / <strong>{REPO_NAME}</strong>
                                </span>
                            </RepoName>
                            <StarHighlight>
                                <StarHighlightInner>
                                    <GithubStarLink
                                        starCount={starCount}
                                        source="welcomeModal"
                                        dataTestId="welcome-modal-github-star-button"
                                    />
                                </StarHighlightInner>
                                <SparkleMark aria-hidden $top={-6} $left={-13} $size={8} $color="textBrand">
                                    <Icon icon={StarFour} weight="fill" size="inherit" />
                                </SparkleMark>
                                <SparkleMark aria-hidden $top={-9} $right={-12} $size={13} $color="tagsTrueYellowIcon">
                                    <Icon icon={StarFour} weight="fill" size="inherit" />
                                </SparkleMark>
                                <SparkleMark aria-hidden $top={21} $right={-18} $size={7} $color="chartsInformationLow">
                                    <Icon icon={StarFour} weight="fill" size="inherit" />
                                </SparkleMark>
                            </StarHighlight>
                        </RepoHeader>
                        <RepoDescription>{t('welcome.githubRepoDescription')}</RepoDescription>
                        <Topics>
                            {REPO_TOPICS.map((topic) => (
                                <Pill
                                    key={topic}
                                    label={topic}
                                    color="primary"
                                    variant="filled"
                                    size="sm"
                                    clickable={false}
                                />
                            ))}
                        </Topics>
                    </RepoCard>
                </BrowserWindow>
                <Stargazers>
                    <AvatarStack aria-hidden>
                        {AVATAR_COLORS.map((color) => (
                            <AvatarDot key={color} $color={color} />
                        ))}
                    </AvatarStack>
                    {starCount === null
                        ? t('welcome.githubStargazersNoCount')
                        : t('welcome.githubStargazers', { stars: formatNumber(starCount) })}
                </Stargazers>
            </Illustration>
        </SlideContainer>
    );
}
