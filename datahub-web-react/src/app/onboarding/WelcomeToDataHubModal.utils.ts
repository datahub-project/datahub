export type TourVideoKey = 'search' | 'lineage' | 'impact' | 'aiDocs';

export function videoKeyForSlide(slideIndex: number, documentationSlideEnabled: boolean): TourVideoKey | null {
    if (slideIndex === 0) return 'search';
    if (slideIndex === 1) return 'lineage';
    if (slideIndex === 2) return 'impact';
    if (documentationSlideEnabled && slideIndex === 3) return 'aiDocs';
    return null;
}

// The slide stays mounted either way. The visible slide and the one being
// prefetched get an MP4 URL. The rest do not download.
export function videoSrcForSlide(
    currentSlide: number,
    slideIndex: number,
    src?: string,
    prefetchSlide?: number | null,
): string | undefined {
    if (!src) return undefined;
    if (currentSlide !== slideIndex && prefetchSlide !== slideIndex) return undefined;
    return src;
}
