import { videoKeyForSlide, videoSrcForSlide } from '@app/onboarding/WelcomeToDataHubModal.utils';

describe('welcome tour video loading', () => {
    it('names a video for each tour slide and none for the closing screenshot', () => {
        expect(videoKeyForSlide(0, false)).toBe('search');
        expect(videoKeyForSlide(1, false)).toBe('lineage');
        expect(videoKeyForSlide(2, false)).toBe('impact');
        expect(videoKeyForSlide(3, false)).toBeNull();
        expect(videoKeyForSlide(3, true)).toBe('aiDocs');
    });

    it('gives the MP4 to the visible slide and the one being prefetched', () => {
        expect(videoSrcForSlide(0, 0, '/search.mp4')).toBe('/search.mp4');
        expect(videoSrcForSlide(0, 1, '/lineage.mp4')).toBeUndefined();
        expect(videoSrcForSlide(0, 2, '/impact.mp4')).toBeUndefined();

        expect(videoSrcForSlide(1, 0, '/search.mp4')).toBeUndefined();
        expect(videoSrcForSlide(1, 1, '/lineage.mp4')).toBe('/lineage.mp4');
        expect(videoSrcForSlide(2, 2, '/impact.mp4')).toBe('/impact.mp4');

        expect(videoSrcForSlide(0, 1, '/lineage.mp4', 1)).toBe('/lineage.mp4');
        expect(videoSrcForSlide(0, 2, '/impact.mp4', 1)).toBeUndefined();
        expect(videoSrcForSlide(0, 0, '')).toBeUndefined();
    });
});
