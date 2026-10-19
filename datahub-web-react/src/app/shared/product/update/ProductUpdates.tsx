import { Button, Heading, Text, Tooltip } from '@components';
import React, { useCallback, useEffect, useLayoutEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router-dom';

import analytics, { EventType } from '@app/analytics';
import { NAV_SIDEBAR_ID, NAV_SIDEBAR_WIDTH_COLLAPSED, NAV_SIDEBAR_WIDTH_EXPANDED } from '@app/shared/constants';
import { checkShouldSkipWelcomeModal, dismissProductUpdate } from '@app/shared/localStorageUtils';
import {
    CTAContainer,
    CloseButton,
    Content,
    FeatureContent,
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
    getDefaultProductUpdateLink,
    getLocalizedReleaseMonth,
    getProductUpdateVersion,
    isProductUpdateHiddenByDismiss,
    readProductUpdateBootstrap,
    resolveLatestProductUpdate,
    resolveProductUpdateImage,
} from '@app/shared/product/update/ProductUpdates.utils';
import {
    useDismissProductAnnouncement,
    useGetLatestProductAnnouncementData,
    useIsProductAnnouncementEnabled,
    useIsProductAnnouncementVisible,
} from '@app/shared/product/update/hooks';
import { isVersionMatch } from '@app/shared/product/update/versionUtils';
import { useIsHomePage } from '@app/shared/useIsHomePage';
import { useAppConfig, useIsShowAcrylInfoEnabled } from '@app/useAppConfig';
import { getRuntimeBasePath } from '@utils/runtimeBasePath';

const PRODUCT_UPDATE_PRELOAD_ATTR = 'data-product-update-preload';

/** Start the hosted image fetch before the toast commits, when the HTML did not already preload it. */
function preloadProductUpdateImage(href: string) {
    const absolute = new URL(href, window.location.href).href;
    const existing = document.head.querySelector(`link[${PRODUCT_UPDATE_PRELOAD_ATTR}]`);
    if (existing instanceof HTMLLinkElement && existing.href === absolute) {
        return;
    }
    existing?.remove();
    const link = document.createElement('link');
    link.rel = 'preload';
    link.as = 'image';
    link.href = href;
    link.setAttribute('fetchpriority', 'high');
    link.setAttribute(PRODUCT_UPDATE_PRELOAD_ATTR, '');
    document.head.appendChild(link);
}

export default function ProductUpdates() {
    const { t, i18n } = useTranslation('shared.product');
    const { t: tc } = useTranslation('common.actions');
    const history = useHistory();
    const isFeatureEnabled = useIsProductAnnouncementEnabled();
    const appConfig = useAppConfig();
    const [bootstrap] = useState(readProductUpdateBootstrap);
    const announcement = useGetLatestProductAnnouncementData();
    const serverUpdate = announcement.status === 'ready' ? announcement.update : null;
    const latestUpdate = resolveLatestProductUpdate(bootstrap, announcement.status, serverUpdate);
    const isCloud = useIsShowAcrylInfoEnabled();
    const isOnHomePage = useIsHomePage();

    const updateId = latestUpdate?.id ?? null;
    const { visible, pending, dismissed, refetch } = useIsProductAnnouncementVisible(updateId);
    const dismiss = useDismissProductAnnouncement(updateId, refetch);

    // Only the id that was dismissed. A new id from GraphQL must not inherit it.
    const [sessionDismissedId, setSessionDismissedId] = useState<string | null>(null);
    const dismissedLocally = isProductUpdateHiddenByDismiss(updateId, sessionDismissedId);
    const [sidebarWidth, setSidebarWidth] = useState(NAV_SIDEBAR_WIDTH_EXPANDED);
    const imageFromPayload =
        serverUpdate && bootstrap && serverUpdate.id !== bootstrap.id ? undefined : bootstrap?.image;
    const imageSrc = resolveProductUpdateImage(imageFromPayload, isCloud);

    useEffect(() => {
        if (!dismissed || !updateId) {
            return;
        }
        dismissProductUpdate(updateId);
        setSessionDismissedId(updateId);
    }, [dismissed, updateId]);

    const featureAllows = appConfig.loaded ? isFeatureEnabled : Boolean(bootstrap);
    const versionMatches = isVersionMatch(appConfig.config.appVersion, latestUpdate?.requiredVersion);
    // The hosted JSON is enough to paint. The step-state query only hides the toast once it
    // confirms this user dismissed it. A slow or failed check must not pull the toast back down.
    const serverAllows = bootstrap ? !dismissed : !pending && visible;
    const showToast =
        featureAllows &&
        isOnHomePage &&
        checkShouldSkipWelcomeModal() &&
        !dismissedLocally &&
        Boolean(latestUpdate?.enabled) &&
        versionMatches &&
        serverAllows;

    useLayoutEffect(() => {
        if (!showToast || !imageSrc) {
            return undefined;
        }
        preloadProductUpdateImage(imageSrc);
        return undefined;
    }, [imageSrc, showToast]);

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
        if (updateId) {
            dismissProductUpdate(updateId);
            setSessionDismissedId(updateId);
        }
        dismiss();
    }, [dismiss, updateId]);

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

    if (!showToast || !latestUpdate) {
        return null;
    }

    const { title, header, description, primaryCtaText, primaryCtaLink, ctaText, ctaLink, features, releaseMonth } =
        latestUpdate;

    // Helper to check if value is actually present (not null, undefined, or string "null")
    const isPresent = (value: any) => value && value !== 'null' && value !== null;

    const version = isCloud ? getProductUpdateVersion(latestUpdate.id) : null;
    const localizedReleaseMonth = getLocalizedReleaseMonth(i18n.language, releaseMonth);
    const defaultTitle = localizedReleaseMonth
        ? t('updates.defaultTitle', {
              month: localizedReleaseMonth,
          })
        : null;
    const contentTitle = isPresent(title) ? title : defaultTitle;
    const displayTitle = isPresent(header) ? header : contentTitle;

    // Only show title in content if header exists (to avoid duplication)
    const showTitleInContent = isPresent(header);

    // Determine primary CTA (prefer new format, fall back to legacy)
    const primaryText = primaryCtaText || ctaText || (version ? t('updates.defaultCtaText', { version }) : null);
    const primaryLink = buildUrl(primaryCtaLink || ctaLink || getDefaultProductUpdateLink(latestUpdate.id, isCloud));

    // Secondary CTA (only if both text and link are present)
    const secondaryText = isPresent(latestUpdate.secondaryCtaText) ? latestUpdate.secondaryCtaText : null;
    const secondaryLink = isPresent(latestUpdate.secondaryCtaLink) ? buildUrl(latestUpdate.secondaryCtaLink) : null;

    // Limit features to 3 max
    const displayFeatures = features && features.length > 0 ? features.slice(0, 3) : null;

    return (
        <ToastContainer $sidebarWidth={sidebarWidth}>
            <Header>
                <Heading type="h3" size="lg" weight="bold">
                    {displayTitle}
                </Heading>
                <Tooltip title={tc('dismiss')} placement="left">
                    <CloseButton onClick={handleDismiss}>
                        <StyledCloseIcon />
                    </CloseButton>
                </Tooltip>
            </Header>
            <Content>
                {description && (
                    <HeroSection>
                        {showTitleInContent && (
                            <Text size="lg" weight="bold">
                                {contentTitle}
                            </Text>
                        )}
                        <Text size="md" weight="medium" color="textSecondary" style={{ lineHeight: '1.4' }}>
                            {description}
                        </Text>
                    </HeroSection>
                )}
                {imageSrc && (
                    <ImageSection>
                        <Image src={imageSrc} alt={contentTitle || undefined} />
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
                                    color="textSecondary"
                                    style={{ textTransform: 'lowercase', whiteSpace: 'nowrap' }}
                                >
                                    {t('updates.moreInThisRelease')}
                                </Text>
                                <SectionHeaderLine />
                            </SectionHeaderContainer>
                        )}
                        <FeatureList>
                            {displayFeatures.map((feature) => {
                                return (
                                    // $hasIcon={false} — icons not used in current product update JSONs
                                    <FeatureItem key={feature.title} $hasIcon={false}>
                                        <FeatureContent>
                                            <Text size="md" weight="semiBold">
                                                {feature.title}
                                            </Text>
                                            <Text size="md" weight="normal" color="textSecondary" lineHeight="xs">
                                                {feature.description}
                                            </Text>
                                            {feature.availability && feature.availability !== 'null' && (
                                                <Text type="span" size="sm" color="textTertiary">
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
                                        // eslint-disable-next-line i18next/no-literal-string -- (untranslated-text) window.open feature string
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
                                color="primary"
                                onClick={() => {
                                    trackClick(primaryLink);
                                    handleDismiss();
                                    if (primaryLink.startsWith('http')) {
                                        // eslint-disable-next-line i18next/no-literal-string -- (untranslated-text) window.open feature string
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
