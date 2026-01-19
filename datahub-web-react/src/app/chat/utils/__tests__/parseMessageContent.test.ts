import { parseMessageContent } from '@app/chat/utils/parseMessageContent';

describe('parseMessageContent', () => {
    describe('plain markdown', () => {
        it('returns single markdown part for plain text', () => {
            const result = parseMessageContent('Hello world');
            expect(result).toEqual([{ type: 'markdown', content: 'Hello world' }]);
        });

        it('handles empty string', () => {
            const result = parseMessageContent('');
            expect(result).toEqual([{ type: 'markdown', content: '' }]);
        });

        it('handles whitespace-only text', () => {
            const result = parseMessageContent('   ');
            expect(result).toEqual([{ type: 'markdown', content: '   ' }]);
        });
    });

    describe('complete code blocks', () => {
        it('extracts a single code block with language', () => {
            const input = '```sql\nSELECT * FROM users\n```';
            const result = parseMessageContent(input);
            expect(result).toEqual([{ type: 'code', language: 'sql', content: 'SELECT * FROM users' }]);
        });

        it('extracts a code block without language identifier', () => {
            const input = '```\nsome code\n```';
            const result = parseMessageContent(input);
            expect(result).toEqual([{ type: 'code', language: 'code', content: 'some code' }]);
        });

        it('extracts code block with markdown before', () => {
            const input = "Here's a query:\n\n```sql\nSELECT * FROM users\n```";
            const result = parseMessageContent(input);
            expect(result).toHaveLength(2);
            expect(result[0]).toEqual({ type: 'markdown', content: "Here's a query:\n\n" });
            expect(result[1]).toEqual({ type: 'code', language: 'sql', content: 'SELECT * FROM users' });
        });

        it('extracts code block with markdown after', () => {
            const input = '```sql\nSELECT * FROM users\n```\n\nThis returns all users.';
            const result = parseMessageContent(input);
            expect(result).toHaveLength(2);
            expect(result[0]).toEqual({ type: 'code', language: 'sql', content: 'SELECT * FROM users' });
            expect(result[1]).toEqual({ type: 'markdown', content: '\n\nThis returns all users.' });
        });

        it('extracts code block with markdown before and after', () => {
            const input = "Here's a query:\n\n```sql\nSELECT * FROM users\n```\n\nThis returns all users.";
            const result = parseMessageContent(input);
            expect(result).toHaveLength(3);
            expect(result[0]).toEqual({ type: 'markdown', content: "Here's a query:\n\n" });
            expect(result[1]).toEqual({ type: 'code', language: 'sql', content: 'SELECT * FROM users' });
            expect(result[2]).toEqual({ type: 'markdown', content: '\n\nThis returns all users.' });
        });

        it('handles multiple code blocks', () => {
            const input = '```python\nprint("hello")\n```\n\nAnd also:\n\n```javascript\nconsole.log("hi")\n```';
            const result = parseMessageContent(input);
            expect(result).toHaveLength(3);
            expect(result[0]).toEqual({ type: 'code', language: 'python', content: 'print("hello")' });
            expect(result[1]).toEqual({ type: 'markdown', content: '\n\nAnd also:\n\n' });
            expect(result[2]).toEqual({ type: 'code', language: 'javascript', content: 'console.log("hi")' });
        });
    });

    describe('incomplete/truncated code blocks', () => {
        it('handles incomplete code block at end', () => {
            const input = 'Query:\n```sql\nSELECT * FROM';
            const result = parseMessageContent(input);
            expect(result).toHaveLength(2);
            expect(result[0]).toEqual({ type: 'markdown', content: 'Query:\n' });
            expect(result[1]).toEqual({ type: 'code', language: 'sql', content: 'SELECT * FROM' });
        });

        it('handles incomplete code block without language', () => {
            const input = 'Code:\n```\nsome partial code';
            const result = parseMessageContent(input);
            expect(result).toHaveLength(2);
            expect(result[0]).toEqual({ type: 'markdown', content: 'Code:\n' });
            expect(result[1]).toEqual({ type: 'code', language: 'code', content: 'some partial code' });
        });

        it('handles just the opening fence with language', () => {
            const input = 'Result:\n```json\n';
            const result = parseMessageContent(input);
            // Empty code content should not be added
            expect(result).toHaveLength(1);
            expect(result[0]).toEqual({ type: 'markdown', content: 'Result:\n' });
        });
    });

    describe('edge cases', () => {
        it('handles code block with multiline content', () => {
            const input = '```python\ndef hello():\n    print("world")\n\nhello()\n```';
            const result = parseMessageContent(input);
            expect(result).toHaveLength(1);
            expect(result[0].type).toBe('code');
            expect(result[0].language).toBe('python');
            expect(result[0].content).toContain('def hello()');
            expect(result[0].content).toContain('print("world")');
        });

        it('handles various language identifiers', () => {
            const languages = ['python', 'javascript', 'typescript', 'sql', 'json', 'yaml', 'bash', 'shell'];
            languages.forEach((lang) => {
                const input = `\`\`\`${lang}\ncode\n\`\`\``;
                const result = parseMessageContent(input);
                expect(result[0].language).toBe(lang);
            });
        });
    });
});
