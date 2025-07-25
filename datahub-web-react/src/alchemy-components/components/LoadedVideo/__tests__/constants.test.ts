import { VIDEO_WIDTH } from '@components/components/LoadedVideo/constants';

describe('LoadedVideo constants', () => {
    it('should be a string value', () => {
        expect(typeof VIDEO_WIDTH).toBe('string');
    });
});
