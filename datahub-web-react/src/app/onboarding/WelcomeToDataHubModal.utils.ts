export type TourVideoKey = 'search' | 'lineage' | 'impact' | 'aiDocs';

export function videoKeyForSlide(slideIndex: number, documentationSlideEnabled: boolean): TourVideoKey | null {
    if (slideIndex === 0) return 'search';
    if (slideIndex === 1) return 'lineage';
    if (slideIndex === 2) return 'impact';
    if (documentationSlideEnabled && slideIndex === 3) return 'aiDocs';
    return null;
}

// The slide stays mounted either way. Only the visible one gets an MP4 URL,
// so the others do not download until the carousel reaches them.
export function videoSrcForSlide(currentSlide: number, slideIndex: number, src?: string): string | undefined {
    if (currentSlide !== slideIndex) return undefined;
    return src || undefined;
}
