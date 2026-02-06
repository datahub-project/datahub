import { Button, Heading, Text, Tooltip } from '@components';
import * as phosphorIcons from '@phosphor-icons/react';
import React, { useCallback, useEffect, useState } from 'react';
import { useHistory } from 'react-router-dom';

import analytics, { EventType } from '@app/analytics';
import { NAV_SIDEBAR_ID, NAV_SIDEBAR_WIDTH_COLLAPSED, NAV_SIDEBAR_WIDTH_EXPANDED } from '@app/shared/constants';
import {
    CTAContainer,
    CloseButton,
    Content,
    FeatureContent,
    FeatureIconWrapper,
    FeatureItem,
    FeatureList,
    FeaturesSection,
    Header,
    HeroSection,
    Image,
    ImageSection,
    SectionHeaderContainer,
    SectionHeaderLine,
    StyledCloseIcon,
    ToastContainer,
} from '@app/shared/product/update/ProductUpdates.components';
import {
    useDismissProductAnnouncement,
    useGetLatestProductAnnouncementData,
    useIsProductAnnouncementEnabled,
    useIsProductAnnouncementVisible,
} from '@app/shared/product/update/hooks';
import { isVersionMatch } from '@app/shared/product/update/versionUtils';
import { convertToPascalCase } from '@app/shared/stringUtils';
import { useIsHomePage } from '@app/shared/useIsHomePage';
import { useAppConfig } from '@app/useAppConfig';
import { getRuntimeBasePath } from '@utils/runtimeBasePath';

export default function ProductUpdates() {
    const history = useHistory();
    const isFeatureEnabled = useIsProductAnnouncementEnabled();
    const latestUpdate = useGetLatestProductAnnouncementData();
    const appConfig = useAppConfig();
    const isOnHomePage = useIsHomePage();

    const { visible, refetch } = useIsProductAnnouncementVisible(latestUpdate?.id);
    const dismiss = useDismissProductAnnouncement(latestUpdate?.id, refetch);

    // Local state to hide immediately on dismiss
    const [isLocallyVisible, setIsLocallyVisible] = useState(false);
    const [sidebarWidth, setSidebarWidth] = useState(NAV_SIDEBAR_WIDTH_EXPANDED);

    // Check if current version matches required version
    const currentVersion = appConfig.config.appVersion;
    const versionMatches = isVersionMatch(currentVersion, latestUpdate?.requiredVersion);

    useEffect(() => {
        setIsLocallyVisible(visible);
    }, [visible]);

    // Measure sidebar width - use MutationObserver for instant updates
    useEffect(() => {
        const sidebar = document.getElementById(NAV_SIDEBAR_ID);
        if (!sidebar) return undefined;

        const updateWidth = () => {
            // Read the target width from data-collapsed attribute (instant, no animation delay)
            const isCollapsed = sidebar.getAttribute('data-collapsed') === 'true';
            const targetWidth = isCollapsed ? NAV_SIDEBAR_WIDTH_COLLAPSED : NAV_SIDEBAR_WIDTH_EXPANDED;
            setSidebarWidth(targetWidth);
        };

        // Set initial width
        updateWidth();

        // Watch for data-collapsed attribute changes on the sidebar element
        const observer = new MutationObserver(() => {
            updateWidth();
        });

        observer.observe(sidebar, {
            attributes: true,
            attributeFilter: ['data-collapsed'],
        });

        return () => {
            observer.disconnect();
        };
    }, []);

    const handleDismiss = useCallback(() => {
        setIsLocallyVisible(false);
        dismiss();
    }, [dismiss]);

    const trackClick = (url: string) => {
        if (!latestUpdate) return;
        analytics.event({
            type: EventType.ClickProductUpdate,
            id: latestUpdate.id,
            url,
        });
    };

    // Helper to build URL with baseUrl for relative paths
    const buildUrl = (url: string | null | undefined): string | null => {
        if (!url) return null;
        // If it's an external URL, use as-is
        if (url.startsWith('http://') || url.startsWith('https://')) {
            return url;
        }
        // If it's a relative URL (starts with /), prepend baseUrl
        if (url.startsWith('/')) {
            const basePath = getRuntimeBasePath();
            return basePath ? `${basePath}${url}` : url;
        }
        // Otherwise return as-is
        return url;
    };

    // Don't show if:
    if (
        !isFeatureEnabled ||
        !isLocallyVisible ||
        !latestUpdate ||
        !latestUpdate.enabled ||
        !versionMatches ||
        !isOnHomePage
    ) {
        return null;
    }

    const { title, header, image, description, primaryCtaText, primaryCtaLink, ctaText, ctaLink, features } =
        latestUpdate;

    // Helper to check if value is actually present (not null, undefined, or string "null")
    const isPresent = (value: any) => value && value !== 'null' && value !== null;

    // Use header if available, otherwise fall back to title
    const displayTitle = isPresent(header) ? header : title;

    // Only show title in content if header exists (to avoid duplication)
    const showTitleInContent = isPresent(header);

    // Determine primary CTA (prefer new format, fall back to legacy)
    const primaryText = primaryCtaText || ctaText;
    const primaryLink = buildUrl(primaryCtaLink || ctaLink);

    // Secondary CTA (only if both text and link are present)
    const secondaryText = isPresent(latestUpdate.secondaryCtaText) ? latestUpdate.secondaryCtaText : null;
    const secondaryLink = isPresent(latestUpdate.secondaryCtaLink) ? buildUrl(latestUpdate.secondaryCtaLink) : null;

    // Limit features to 3 max
    const displayFeatures = features && features.length > 0 ? features.slice(0, 3) : null;

    return (
        <ToastContainer $sidebarWidth={sidebarWidth}>
            <Header>
                <Heading type="h3" size="lg" weight="bold" color="gray" colorLevel={600}>
                    {displayTitle}
                </Heading>
                <Tooltip title="Dismiss" placement="left">
                    <CloseButton onClick={handleDismiss}>
                        <StyledCloseIcon />
                    </CloseButton>
                </Tooltip>
            </Header>
            <Content>
                {description && (
                    <HeroSection>
                        {showTitleInContent && (
                            <Text size="lg" weight="bold" color="gray" colorLevel={600}>
                                {title}
                            </Text>
                        )}
                        <Text size="md" weight="medium" color="gray" colorLevel={500} style={{ lineHeight: '1.4' }}>
                            {description}
                        </Text>
                    </HeroSection>
                )}
                {image && (
                    <ImageSection>
                        <Image src={image} alt={title} />
                    </ImageSection>
                )}
                {displayFeatures && displayFeatures.length > 0 && (
                    <FeaturesSection>
                        {displayFeatures.length > 1 && (
                            <SectionHeaderContainer>
                                <SectionHeaderLine />
                                <Text
                                    size="sm"
                                    weight="bold"
                                    color="gray"
                                    colorLevel={500}
                                    style={{ textTransform: 'lowercase', whiteSpace: 'nowrap' }}
                                >
                                    more in this release
                                </Text>
                                <SectionHeaderLine />
                            </SectionHeaderContainer>
                        )}
                        <FeatureList>
                            {displayFeatures.map((feature) => {
                                // Try the icon name as-is first, then try converting from kebab-case
                                const iconName = feature.icon;
                                let IconComponent = iconName
                                    ? (phosphorIcons[iconName as keyof typeof phosphorIcons] as
                                          | React.ComponentType<{ size?: number; weight?: string }>
                                          | undefined)
                                    : undefined;

                                // If not found and contains hyphens, try converting to PascalCase
                                if (!IconComponent && iconName?.includes('-')) {
                                    const pascalCaseName = convertToPascalCase(iconName);
                                    IconComponent = phosphorIcons[pascalCaseName as keyof typeof phosphorIcons] as
                                        | React.ComponentType<{ size?: number; weight?: string }>
                                        | undefined;
                                }

                                // Debug logging for icon resolution
                                if (feature.icon && !IconComponent) {
                                    // eslint-disable-next-line no-console
                                    console.warn(`[ProductUpdates] Icon "${feature.icon}" not found in phosphor-icons`);
                                }

                                const hasIcon = !!IconComponent;

                                return (
                                    <FeatureItem key={feature.title} $hasIcon={hasIcon}>
                                        {hasIcon && IconComponent && (
                                            <FeatureIconWrapper>
                                                <IconComponent size={20} weight="regular" />
                                            </FeatureIconWrapper>
                                        )}
                                        <FeatureContent>
                                            <Text size="md" weight="semiBold" color="gray" colorLevel={600}>
                                                {feature.title}
                                            </Text>
                                            <Text
                                                size="md"
                                                weight="normal"
                                                color="gray"
                                                colorLevel={500}
                                                lineHeight="xs"
                                            >
                                                {feature.description}
                                            </Text>
                                            {feature.availability && feature.availability !== 'null' && (
                                                <Text type="span" size="sm" color="gray" colorLevel={400}>
                                                    {feature.availability}
                                                </Text>
                                            )}
                                        </FeatureContent>
                                    </FeatureItem>
                                );
                            })}
                        </FeatureList>
                    </FeaturesSection>
                )}
                {(primaryText && primaryLink) || (secondaryText && secondaryLink) ? (
                    <CTAContainer>
                        {secondaryText && secondaryLink && (
                            <Button
                                variant="secondary"
                                onClick={() => {
                                    trackClick(secondaryLink);
                                    if (secondaryLink.startsWith('http')) {
                                        window.open(secondaryLink, '_blank', 'noopener,noreferrer');
                                    } else {
                                        history.push(secondaryLink);
                                    }
                                }}
                                color="gray"
                            >
                                {secondaryText}
                            </Button>
                        )}
                        {primaryText && primaryLink && (
                            <Button
                                variant="filled"
                                color="violet"
                                onClick={() => {
                                    trackClick(primaryLink);
                                    handleDismiss();
                                    if (primaryLink.startsWith('http')) {
                                        window.open(primaryLink, '_blank', 'noopener,noreferrer');
                                    } else {
                                        history.push(primaryLink);
                                    }
                                }}
                            >
                                {primaryText}
                            </Button>
                        )}
                    </CTAContainer>
                ) : null}
            </Content>
        </ToastContainer>
    );
}
