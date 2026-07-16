import { downloadImage } from '@app/lineageV3/utils/lineageUtils';

describe('DownloadLineageScreenshotButton', () => {
    describe('Entity name cleaning', () => {
        it('should clean special characters', () => {
            const cleanName = (name: string) => name.replace(/[^a-zA-Z0-9_-]/g, '_');

            expect(cleanName('dataset-with/special@chars#and$symbols')).toBe('dataset-with_special_chars_and_symbols');
            expect(cleanName('user.transactions')).toBe('user_transactions');
            expect(cleanName('normal_name')).toBe('normal_name');
            expect(cleanName('123-valid_name')).toBe('123-valid_name');
        });
    });

    describe('downloadImage', () => {
        let mockAnchorElement: any;
        let originalCreateElement: typeof document.createElement;

        beforeEach(() => {
            // Mock anchor element
            mockAnchorElement = {
                setAttribute: vi.fn(),
                click: vi.fn(),
            };

            // Mock document.createElement
            originalCreateElement = document.createElement;
            document.createElement = vi.fn().mockReturnValue(mockAnchorElement);
        });

        afterEach(() => {
            document.createElement = originalCreateElement;
        });

        it('should create anchor element and set download attribute with default filename', () => {
            const dataUrl = 'data:image/png;base64,mockdata';

            downloadImage(dataUrl);

            expect(document.createElement).toHaveBeenCalledWith('a');
            expect(mockAnchorElement.setAttribute).toHaveBeenCalledWith('href', dataUrl);
            expect(mockAnchorElement.setAttribute).toHaveBeenCalledWith(
                'download',
                expect.stringMatching(/^reactflow_\d{4}-\d{2}-\d{2}_\d{6}\.png$/),
            );
            expect(mockAnchorElement.click).toHaveBeenCalled();
        });

        it('should handle empty string name parameter and use default prefix', () => {
            const dataUrl = 'data:image/png;base64,mockdata';

            downloadImage(dataUrl, '');

            expect(mockAnchorElement.setAttribute).toHaveBeenCalledWith(
                'download',
                expect.stringMatching(/^reactflow_\d{4}-\d{2}-\d{2}_\d{6}\.png$/),
            );
        });

        it('should generate filename with correct format and timestamp', () => {
            const dataUrl = 'data:image/png;base64,mockdata';
            const name = 'test_entity';

            downloadImage(dataUrl, name);

            // Verify that the anchor element is created and the href attribute is set
            expect(mockAnchorElement.setAttribute).toHaveBeenCalledWith('href', dataUrl);
            expect(mockAnchorElement.click).toHaveBeenCalledTimes(1);

            // Get the download filename from the setAttribute calls
            const setAttributeCalls = mockAnchorElement.setAttribute.mock.calls;
            const downloadCall = setAttributeCalls.find((call: any[]) => call[0] === 'download');
            const filename = downloadCall[1];

            // Verify filename format: test_entity_YYYY-MM-DD_HHMMSS.png
            expect(filename).toMatch(/^test_entity_\d{4}-\d{2}-\d{2}_\d{6}\.png$/);

            // Extract and verify date part (YYYY-MM-DD)
            const parts = filename.split('_');
            const datePart = parts[2];
            expect(datePart).toMatch(/^\d{4}-\d{2}-\d{2}$/);

            // Extract and verify time part (HHMMSS)
            const timePart = parts[3].replace('.png', '');
            expect(timePart).toMatch(/^\d{6}$/);
            expect(timePart.length).toBe(6);
        });
    });
});
