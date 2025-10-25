import { getActiveMention } from '@app/chat/utils/mentionUtils';

describe('mentionUtils', () => {
    describe('getActiveMention', () => {
        it('should return null for text without @ symbol', () => {
            const textContent = 'Hello world';
            const cursorPos = 5;

            const result = getActiveMention(textContent, cursorPos);

            expect(result).toBeNull();
        });

        it('should return null for @ followed by space', () => {
            const textContent = 'Hello @ world';
            const cursorPos = 8;

            const result = getActiveMention(textContent, cursorPos);

            expect(result).toBeNull();
        });

        it('should return null for @ followed by newline', () => {
            const textContent = 'Hello @\nworld';
            const cursorPos = 8;

            const result = getActiveMention(textContent, cursorPos);

            expect(result).toBeNull();
        });

        it('should return mention info for active @ mention', () => {
            const textContent = 'Hello @user world';
            const cursorPos = 11; // caret after 'r'

            const result = getActiveMention(textContent, cursorPos);

            expect(result).toEqual({
                startIndex: 6,
                query: 'user',
            });
        });

        it('should return mention info for @ at beginning', () => {
            const textContent = '@user world';
            const cursorPos = 5; // caret after 'r'

            const result = getActiveMention(textContent, cursorPos);

            expect(result).toEqual({
                startIndex: 0,
                query: 'user',
            });
        });

        it('should return mention info for @ at end', () => {
            const textContent = 'Hello @user';
            const cursorPos = 11;

            const result = getActiveMention(textContent, cursorPos);

            expect(result).toEqual({
                startIndex: 6,
                query: 'user',
            });
        });

        it('should return mention info for partial query', () => {
            const textContent = 'Hello @us world';
            const cursorPos = 9;

            const result = getActiveMention(textContent, cursorPos);

            expect(result).toEqual({
                startIndex: 6,
                query: 'us',
            });
        });

        it('should return mention info for multiple @ symbols (last one)', () => {
            const textContent = 'Hello @user1 and @user2 world';
            const cursorPos = 23; // caret right after '2'

            const result = getActiveMention(textContent, cursorPos);

            expect(result).toEqual({
                startIndex: 17,
                query: 'user2',
            });
        });

        it('should return null for @ at cursor position but with space after', () => {
            const textContent = 'Hello @ user world';
            const cursorPos = 8;

            const result = getActiveMention(textContent, cursorPos);

            expect(result).toBeNull();
        });

        it('should handle empty text', () => {
            const textContent = '';
            const cursorPos = 0;

            const result = getActiveMention(textContent, cursorPos);

            expect(result).toBeNull();
        });

        it('should handle cursor position beyond text length', () => {
            const textContent = 'Hello @user';
            const cursorPos = 20;

            const result = getActiveMention(textContent, cursorPos);

            expect(result).toEqual({
                startIndex: 6,
                query: 'user',
            });
        });
    });
});
