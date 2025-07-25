describe('Entity name cleaning', () => {
    it('should clean special characters', () => {
        const cleanName = (name: string) => name.replace(/[^a-zA-Z0-9_-]/g, '_');

        expect(cleanName('dataset-with/special@chars#and$symbols')).toBe('dataset-with_special_chars_and_symbols');
        expect(cleanName('user.transactions')).toBe('user_transactions');
        expect(cleanName('normal_name')).toBe('normal_name');
        expect(cleanName('123-valid_name')).toBe('123-valid_name');
    });
});
