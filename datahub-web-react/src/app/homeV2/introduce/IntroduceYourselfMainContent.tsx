import { Button, SimpleSelect, Tooltip, toast } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { Check } from '@phosphor-icons/react/dist/csr/Check';
import { GearSix } from '@phosphor-icons/react/dist/csr/GearSix';
import { User } from '@phosphor-icons/react/dist/csr/User';
import orderBy from 'lodash/orderBy';
import React, { useContext, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { useGetDataPlatforms } from '@app/homeV2/content/tabs/discovery/sections/platform/useGetDataPlatforms';
import { PLATFORMS_MODULE_ID } from '@app/homeV2/content/tabs/discovery/sections/platform/useGetPlatforms';
import { PERSONA_TYPE_TO_VIEW_URN, PersonaType, ROLE_TO_PERSONA_TYPE } from '@app/homeV2/shared/types';
import OnboardingContext from '@app/onboarding/OnboardingContext';
import Loading from '@app/shared/Loading';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useListGlobalViewsQuery } from '@src/graphql/view.generated';

import { useListRecommendationsQuery } from '@graphql/recommendations.generated';
import { useUpdateCorpUserPropertiesMutation, useUpdateCorpUserViewsSettingsMutation } from '@graphql/user.generated';
import { DataHubViewType, DataPlatform, EntityType, ScenarioType } from '@types';

const Container = styled.div`
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 0 52px;
`;

const Content = styled.div`
    background-color: ${(props) => props.theme.colors.bg};
    padding: 20px;
`;

const Title = styled.div`
    color: ${(props) => props.theme.colors.text};
    text-align: center;
    font: 700 35px Mulish;
    line-height: 44px;
    margin-bottom: 3px;
`;

const Subtitle = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
    width: 268px;
    text-align: center;
    font: 400 13px Mulish;
    line-height: 21px;
    opacity: 0.6;
    margin-bottom: 28px;
`;

const DoneButton = styled(Button)`
    width: 290px;
    height: 45px;
    flex-shrink: 0;
    margin-top: 12px;
`;

const SelectWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: flex-start;
    position: relative;
    width: 290px;

    & + & {
        margin-top: 12px;
    }
`;

const LeadingIcon = styled.div`
    position: absolute;
    left: 10px;
    z-index: 2;
    display: flex;
    align-items: center;
    color: ${(props) => props.theme.colors.textTertiary};
    pointer-events: none;
`;

const RoleSelect = styled(SimpleSelect)`
    width: 290px;

    & > div {
        padding-left: 28px;
    }
`;

const PlatformSelectTrigger = styled.button<{ $open?: boolean }>`
    width: 290px;
    min-height: 42px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    padding: 6px 12px 6px 36px;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 8px;
    background: ${(props) => props.theme.colors.bg};
    color: ${(props) => props.theme.colors.textTertiary};
    cursor: pointer;
    text-align: left;
`;

const PlatformTags = styled.div`
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 4px;
    flex: 1;
    min-width: 0;
`;

const PlatformPlaceholder = styled.span`
    color: ${(props) => props.theme.colors.textTertiary};
    font-size: 14px;
`;

const OverflowTag = styled.span`
    font-size: 12px;
    line-height: 20px;
    color: ${(props) => props.theme.colors.textSecondary};
    padding-left: 5px;
`;

const DropdownCaret = styled(CaretDown)<{ $open?: boolean }>`
    flex-shrink: 0;
    transition: transform 0.15s ease;
    transform: ${(props) => (props.$open ? 'rotate(180deg)' : 'none')};
`;

const PlatformDropdown = styled.div<{ $maxHeight: number }>`
    position: absolute;
    top: calc(100% + 4px);
    left: 0;
    z-index: 100;
    width: 290px;
    max-height: ${(props) => props.$maxHeight}px;
    overflow: auto;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 8px;
    background: ${(props) => props.theme.colors.bg};
    box-shadow: ${(props) => props.theme.colors.shadowMd};
`;

const SelectGrid = styled.div`
    max-width: 290px;
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    grid-gap: 10px;
    padding: 10px;
`;

const SelectOption = styled.button`
    display: flex;
    position: relative;
    overflow: hidden;
    width: 40px;
    height: 40px;
    padding: 0;
    border: none;
    background: ${(props) => props.theme.colors.bg};
    cursor: pointer;
`;

const SelectTag = styled.div`
    margin-right: 4px;
    display: flex;
    align-items: center;
`;

const PsuedoCheckBox = styled.div<{ checked?: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    position: absolute;
    top: 0;
    left: 0;
    width: 12px;
    height: 12px;
    border-radius: 4px;
    border: 1px solid ${(props) => props.theme.colors.border};
    background: ${(props) => props.theme.colors.bgSurface};
    color: ${(props) => props.theme.colors.textOnFillBrand};

    ${(props) =>
        props.checked &&
        `
        background: ${props.theme.colors.buttonFillBrand};
        border: none;
    `}

    & svg {
        width: 10px;
        height: 10px;
    }
`;

const Footer = styled.div`
    margin-top: 16px;
    display: flex;
    justify-content: center;
    align-items: center;
`;

const SkipButton = styled.div`
    color: ${(props) => props.theme.colors.textTertiary};
    font-weight: 700;
    :hover {
        cursor: pointer;
    }
`;

const DEFAULT_PERSONA = PersonaType.TECHNICAL_USER;
const MAX_VISIBLE_PLATFORM_TAGS = 5;

// TODO: Make section ordering dynamic based on populated data.
export const IntroduceYourselfMainContent = () => {
    const { t } = useTranslation('home.v2');
    const { t: tc } = useTranslation('common.actions');
    const userContext = useUserContext();
    const { refetchUser, user } = userContext;
    const defaultDataPlatforms = useGetDataPlatforms();
    const [updateCorpUserMutation, { loading }] = useUpdateCorpUserPropertiesMutation();
    const [updateUserViewSettingMutation] = useUpdateCorpUserViewsSettingsMutation();

    const history = useHistory();
    const authenticatedUser = useUserContext();
    const currentUserUrn = authenticatedUser?.user?.urn || '';
    const entityRegistry = useEntityRegistry();

    // commented out for now, but may be brought back in the future
    // const [selectedPersona, setSelectedPersona] = useState<string>(PersonaType.TECHNICAL_USER);
    const selectedPersona = PersonaType.TECHNICAL_USER;
    const [selectedPlatforms, setSelectedPlatforms] = useState<string[]>([]);
    const [selectedTitle, setSelectedTitle] = useState('');
    const [isPlatformDropdownOpen, setIsPlatformDropdownOpen] = useState(false);
    const platformSelectRef = useRef<HTMLDivElement | null>(null);

    const { loading: viewsLoading, data: globalViewsData } = useListGlobalViewsQuery({
        variables: {
            start: 0,
            count: 100,
        },
        fetchPolicy: 'no-cache',
    });

    const globalViews = globalViewsData?.listGlobalViews?.views || [];

    const { data, loading: reccosLoading } = useListRecommendationsQuery({
        variables: {
            input: {
                userUrn: currentUserUrn as string,
                requestContext: {
                    scenario: ScenarioType.Home,
                },
                limit: 10,
            },
        },
        fetchPolicy: 'cache-first',
        skip: !currentUserUrn,
    });

    const platformsModule = data?.listRecommendations?.modules?.find(
        (module) => module.moduleId === PLATFORMS_MODULE_ID,
    );

    const getPlatformList = () => {
        if (platformsModule && platformsModule.content.length) {
            return platformsModule?.content
                ?.filter((content) => content.entity)
                .map((content) => ({
                    count: content.params?.contentParams?.count || 0,
                    platform: content.entity as DataPlatform,
                }));
        }
        return defaultDataPlatforms;
    };

    const platforms = getPlatformList();

    const handleRoleChange = (values: string[]) => {
        setSelectedTitle(values[0] || '');
    };

    const togglePlatform = (urn: string) => {
        setSelectedPlatforms((prev) => (prev.includes(urn) ? prev.filter((value) => value !== urn) : [...prev, urn]));
    };

    useEffect(() => {
        if (!isPlatformDropdownOpen) {
            return undefined;
        }
        const handleClickOutside = (event: MouseEvent) => {
            if (platformSelectRef.current && !platformSelectRef.current.contains(event.target as Node)) {
                setIsPlatformDropdownOpen(false);
            }
        };
        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, [isPlatformDropdownOpen]);

    const { setIsUserInitializing } = useContext(OnboardingContext);

    /**
     * Updates the User's Personal Default View via mutation.
     *
     * Then updates the User Context state to contain the new default.
     */
    const setUserDefault = (viewUrn: string | null) => {
        return updateUserViewSettingMutation({
            variables: {
                input: {
                    defaultView: viewUrn,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    userContext.updateState({
                        ...userContext.state,
                        views: {
                            ...userContext.state.views,
                            personalDefaultViewUrn: viewUrn,
                        },
                    });
                    userContext.updateLocalState({
                        ...userContext.localState,
                        selectedViewUrn: viewUrn,
                    });
                    analytics.event({
                        type: EventType.SetUserDefaultViewEvent,
                        urn: viewUrn,
                        viewType: (viewUrn && DataHubViewType.Global) || null,
                    });
                }
            })
            .catch((_) => {
                toast.destroy();
                toast.error(t('introduceYourself.errorProvisionView'), { duration: 3 });
            });
    };

    const onSubmitDetails = () => {
        setIsUserInitializing(true);

        // The default views for each persona needs to be created prior
        const personaDefaultView = globalViews.find((view) => view.urn === PERSONA_TYPE_TO_VIEW_URN[selectedPersona]);
        const { globalDefaultViewUrn } = userContext.state.views;

        if (personaDefaultView && !globalDefaultViewUrn) {
            setUserDefault(personaDefaultView.urn);
        }

        updateCorpUserMutation({
            variables: {
                urn: user?.urn as string,
                input: {
                    personaUrn: selectedPersona,
                    platformUrns: selectedPlatforms,
                    title: selectedTitle,
                },
            },
        })
            .then(async () => {
                analytics.event({
                    type: EventType.IntroduceYourselfSubmitEvent,
                    role: selectedPersona,
                    platformUrns: selectedPlatforms || [],
                });
                await refetchUser();
                history.push('/');
            })
            .catch((err) => {
                console.error(err);
                toast.error(t('introduceYourself.errorSaveDetails'));
            });
    };

    const onSkip = () => {
        setIsUserInitializing(true);

        updateCorpUserMutation({
            variables: {
                urn: user?.urn as string,
                input: {
                    personaUrn: DEFAULT_PERSONA,
                    platformUrns: [],
                },
            },
        })
            .then(async () => {
                analytics.event({
                    type: EventType.IntroduceYourselfSkipEvent,
                });
                await refetchUser();
                history.push('/');
            })
            .catch((err) => {
                console.error(err);
                toast.error(t('introduceYourself.errorSaveDetails'));
            });
    };

    const hasPersona = !!selectedPersona;

    // Sort Roles Alphabetically, then move 'Other' to the end
    const roleOptions = useMemo(() => {
        const sortedRoles = orderBy(Object.keys(ROLE_TO_PERSONA_TYPE), (role) => role);
        const roles = sortedRoles.filter((role) => role !== 'Other');
        roles.push('Other');
        return roles.map((role) => ({
            label: role,
            value: role,
        }));
    }, []);

    // Get window height
    const windowHeight = window.innerHeight;
    const smallWindow = windowHeight <= 719;

    const selectedPlatformEntities = selectedPlatforms
        .map((urn) => platforms.find((platform) => platform.platform.urn === urn)?.platform)
        .filter((platform): platform is DataPlatform => !!platform);
    const visiblePlatformTags = selectedPlatformEntities.slice(0, MAX_VISIBLE_PLATFORM_TAGS);
    const overflowPlatformCount = selectedPlatformEntities.length - visiblePlatformTags.length;

    // Show loading state
    const isLoading = loading && reccosLoading && viewsLoading;
    if (isLoading) return <Loading />;

    return (
        <Container>
            <Content>
                <Title>{t('introduceYourself.mainTitle')}</Title>
                <Subtitle>{t('introduceYourself.mainSubtitle')}</Subtitle>
                <SelectWrapper>
                    <LeadingIcon>
                        <User size={18} />
                    </LeadingIcon>
                    <RoleSelect
                        placeholder={t('introduceYourself.rolePlaceholder')}
                        dataTestId="introduce-role-select"
                        size="lg"
                        width={290}
                        values={selectedTitle ? [selectedTitle] : []}
                        onUpdate={handleRoleChange}
                        showSearch
                        showClear={false}
                        options={roleOptions}
                        optionDataTestId={(option) => `role-option-${option.value}`}
                    />
                </SelectWrapper>
                <SelectWrapper ref={platformSelectRef}>
                    <LeadingIcon>
                        <GearSix size={18} />
                    </LeadingIcon>
                    <PlatformSelectTrigger
                        type="button"
                        data-testid="introduce-data-source-select"
                        $open={isPlatformDropdownOpen}
                        onClick={() => setIsPlatformDropdownOpen((open) => !open)}
                        aria-expanded={isPlatformDropdownOpen}
                    >
                        <PlatformTags>
                            {visiblePlatformTags.length === 0 ? (
                                <PlatformPlaceholder>{t('introduceYourself.dataToolsPlaceholder')}</PlatformPlaceholder>
                            ) : (
                                visiblePlatformTags.map((platform) => (
                                    <SelectTag key={platform.urn}>
                                        <PlatformIcon platform={platform} size={14} />
                                    </SelectTag>
                                ))
                            )}
                            {overflowPlatformCount > 0 && <OverflowTag>{overflowPlatformCount}+</OverflowTag>}
                        </PlatformTags>
                        <DropdownCaret size={16} $open={isPlatformDropdownOpen} />
                    </PlatformSelectTrigger>
                    {isPlatformDropdownOpen && (
                        <PlatformDropdown $maxHeight={smallWindow ? 100 : 300}>
                            <SelectGrid>
                                {platforms.map((platform) => {
                                    const { urn } = platform.platform;
                                    const isChecked = selectedPlatforms.includes(urn);
                                    const displayName = entityRegistry.getDisplayName(
                                        EntityType.DataPlatform,
                                        platform.platform,
                                    );
                                    const platformNameForTestId =
                                        platform.platform.name?.toLowerCase().replace(/\s+/g, '-') || '';
                                    return (
                                        <SelectOption
                                            key={urn}
                                            type="button"
                                            data-testid={`platform-option-${platformNameForTestId}`}
                                            onClick={() => togglePlatform(urn)}
                                            aria-pressed={isChecked}
                                        >
                                            <Tooltip title={displayName} placement="left" mouseEnterDelay={0.5}>
                                                <PsuedoCheckBox checked={isChecked}>
                                                    {isChecked && <Check size={10} weight="bold" />}
                                                </PsuedoCheckBox>
                                                <PlatformIcon
                                                    platform={platform.platform}
                                                    size={24}
                                                    styles={{ width: '40px', height: '40px' }}
                                                />
                                            </Tooltip>
                                        </SelectOption>
                                    );
                                })}
                            </SelectGrid>
                        </PlatformDropdown>
                    )}
                </SelectWrapper>
                {/* Note: This is commented out for now, but may be brought back in the future. As of today, it causes more confusion than it helps */}
                {/* <PersonaSelector selectedPersona={selectedPersona} onSelect={setSelectedPersona} /> */}
                <DoneButton
                    variant="filled"
                    size="lg"
                    onClick={onSubmitDetails}
                    isLoading={loading}
                    disabled={!hasPersona}
                >
                    {t('introduceYourself.getStarted')}
                </DoneButton>
                <Footer>
                    <Tooltip placement="bottom" title={t('introduceYourself.continueTo')}>
                        <SkipButton onClick={onSkip}>{tc('skip')}</SkipButton>
                    </Tooltip>
                </Footer>
            </Content>
        </Container>
    );
};
