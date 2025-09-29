import { stripDotSlashFromAssets } from 'vite.config';
import { beforeEach, describe, expect, it } from 'vitest';

describe('stripDotSlashFromAssets', () => {
    describe('plugin structure', () => {
        it('should return a valid Vite plugin object', () => {
            const plugin = stripDotSlashFromAssets();

            expect(plugin).toEqual({
                name: 'strip-dot-slash',
                transformIndexHtml: expect.any(Function),
            });
        });

        it('should have the correct plugin name', () => {
            const plugin = stripDotSlashFromAssets();
            expect(plugin.name).toBe('strip-dot-slash');
        });
    });

    describe('transformIndexHtml functionality', () => {
        let transformFn: (html: string) => string;

        beforeEach(() => {
            const plugin = stripDotSlashFromAssets();
            transformFn = plugin.transformIndexHtml;
        });

        describe('src attribute transformations', () => {
            it('should remove "./" from src attributes', () => {
                const input = '<img src="./assets/image.png" alt="test">';
                const expected = '<img src="assets/image.png" alt="test">';

                expect(transformFn(input)).toBe(expected);
            });

            it('should handle multiple src attributes in the same HTML', () => {
                const input = `
                    <img src="./assets/image1.png" alt="image1">
                    <script src="./assets/script.js"></script>
                    <img src="./assets/image2.png" alt="image2">
                `;
                const expected = `
                    <img src="assets/image1.png" alt="image1">
                    <script src="assets/script.js"></script>
                    <img src="assets/image2.png" alt="image2">
                `;

                expect(transformFn(input)).toBe(expected);
            });

            it('should not modify src attributes without "./" prefix', () => {
                const input = '<img src="assets/image.png" alt="test">';

                expect(transformFn(input)).toBe(input);
            });

            it('should not modify absolute URLs in src attributes', () => {
                const input = '<img src="https://example.com/image.png" alt="test">';

                expect(transformFn(input)).toBe(input);
            });

            it('should not modify relative paths that don\'t start with "./"', () => {
                const input = '<img src="../assets/image.png" alt="test">';

                expect(transformFn(input)).toBe(input);
            });
        });

        describe('href attribute transformations', () => {
            it('should remove "./" from href attributes', () => {
                const input = '<link href="./assets/styles.css" rel="stylesheet">';
                const expected = '<link href="assets/styles.css" rel="stylesheet">';

                expect(transformFn(input)).toBe(expected);
            });

            it('should handle multiple href attributes in the same HTML', () => {
                const input = `
                    <link href="./assets/styles1.css" rel="stylesheet">
                    <a href="./page.html">Link</a>
                    <link href="./assets/styles2.css" rel="stylesheet">
                `;
                const expected = `
                    <link href="assets/styles1.css" rel="stylesheet">
                    <a href="page.html">Link</a>
                    <link href="assets/styles2.css" rel="stylesheet">
                `;

                expect(transformFn(input)).toBe(expected);
            });

            it('should not modify href attributes without "./" prefix', () => {
                const input = '<link href="assets/styles.css" rel="stylesheet">';

                expect(transformFn(input)).toBe(input);
            });

            it('should not modify absolute URLs in href attributes', () => {
                const input = '<a href="https://example.com">External Link</a>';

                expect(transformFn(input)).toBe(input);
            });

            it('should not modify relative paths that don\'t start with "./"', () => {
                const input = '<a href="../page.html">Parent Page</a>';

                expect(transformFn(input)).toBe(input);
            });
        });

        describe('mixed attribute transformations', () => {
            it('should handle both src and href attributes in the same HTML', () => {
                const input = `
                    <html>
                        <head>
                            <link href="./assets/styles.css" rel="stylesheet">
                            <script src="./assets/script.js"></script>
                        </head>
                        <body>
                            <img src="./assets/image.png" alt="test">
                            <a href="./page.html">Link</a>
                        </body>
                    </html>
                `;
                const expected = `
                    <html>
                        <head>
                            <link href="assets/styles.css" rel="stylesheet">
                            <script src="assets/script.js"></script>
                        </head>
                        <body>
                            <img src="assets/image.png" alt="test">
                            <a href="page.html">Link</a>
                        </body>
                    </html>
                `;

                expect(transformFn(input)).toBe(expected);
            });

            it('should handle elements with both src and href attributes', () => {
                // This is an edge case - elements typically don't have both, but testing robustness
                const input = '<element src="./assets/file1.js" href="./assets/file2.css">';
                const expected = '<element src="assets/file1.js" href="assets/file2.css">';

                expect(transformFn(input)).toBe(expected);
            });
        });

        describe('edge cases', () => {
            it('should handle empty HTML string', () => {
                const input = '';

                expect(transformFn(input)).toBe('');
            });

            it('should handle HTML with no src or href attributes', () => {
                const input = '<div><p>Hello World</p></div>';

                expect(transformFn(input)).toBe(input);
            });

            it('should handle malformed HTML gracefully', () => {
                const input = '<img src="./assets/image.png" alt="test"';
                const expected = '<img src="assets/image.png" alt="test"';

                expect(transformFn(input)).toBe(expected);
            });

            it('should handle attributes with single quotes', () => {
                const input = "<img src='./assets/image.png' alt='test'>";
                // The regex only matches double quotes, so single quotes should remain unchanged
                expect(transformFn(input)).toBe(input);
            });

            it('should handle nested quotes correctly', () => {
                const input = '<img src="./assets/image.png" alt="Image with &quot;quotes&quot;">';
                const expected = '<img src="assets/image.png" alt="Image with &quot;quotes&quot;">';

                expect(transformFn(input)).toBe(expected);
            });

            it('should handle multiple "./" patterns in the same attribute', () => {
                // This is unlikely but testing edge case
                const input = '<img src="././assets/image.png" alt="test">';
                const expected = '<img src="./assets/image.png" alt="test">';

                expect(transformFn(input)).toBe(expected);
            });

            it('should handle whitespace around attributes', () => {
                const input = '<img  src="./assets/image.png"  alt="test"  >';
                const expected = '<img  src="assets/image.png"  alt="test"  >';

                expect(transformFn(input)).toBe(expected);
            });
        });

        describe('real-world scenarios', () => {
            it('should handle a typical index.html structure', () => {
                const input = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DataHub</title>
    <link rel="icon" href="./assets/favicon.ico">
    <link rel="stylesheet" href="./assets/main.css">
    <script src="./assets/config.js"></script>
</head>
<body>
    <div id="root"></div>
    <script src="./assets/main.js"></script>
    <img src="./assets/logo.png" alt="DataHub Logo">
</body>
</html>`;

                const expected = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DataHub</title>
    <link rel="icon" href="assets/favicon.ico">
    <link rel="stylesheet" href="assets/main.css">
    <script src="assets/config.js"></script>
</head>
<body>
    <div id="root"></div>
    <script src="assets/main.js"></script>
    <img src="assets/logo.png" alt="DataHub Logo">
</body>
</html>`;

                expect(transformFn(input)).toBe(expected);
            });

            it('should preserve non-asset references', () => {
                const input = `
                    <link href="./assets/styles.css" rel="stylesheet">
                    <a href="https://datahub.io">External</a>
                    <a href="/absolute/path">Absolute</a>
                    <a href="../relative/path">Relative</a>
                    <img src="data:image/png;base64,iVBOR..." alt="inline">
                `;
                const expected = `
                    <link href="assets/styles.css" rel="stylesheet">
                    <a href="https://datahub.io">External</a>
                    <a href="/absolute/path">Absolute</a>
                    <a href="../relative/path">Relative</a>
                    <img src="data:image/png;base64,iVBOR..." alt="inline">
                `;

                expect(transformFn(input)).toBe(expected);
            });

            it('should handle a typical index.html structure', () => {
                const input = `<!DOCTYPE html>
<html lang="en">
    <head>
        <meta charset="utf-8" />
        <base href="@basePath" />
        <link rel="icon" href="assets/icons/favicon.ico" id="favicon-link" />
        <link rel="mask-icon" href="assets/icons/favicon.ico" id="mask-icon-link" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <meta name="theme-color" content="#000000" />
        <meta name="description" content="A Metadata Platform for the Modern Data Stack" />
        <!--
          manifest.json provides metadata used when your web app is installed on a
          user's mobile device or desktop. See https://developers.google.com/web/fundamentals/web-app-manifest/
        -->
        <link rel="manifest" href="manifest.json" id="manifest-link" />
		<script type="module" crossorigin="" src="./assets/index-K056Uwn8.js"></script>
		<link rel="stylesheet" crossorigin="" href="./assets/index-Byck28hc.css">
        <title>DataHub</title>
    </head>
    <body>
        <noscript>You need to enable JavaScript to run this app.</noscript>
        <div id="root" style="min-height: 100%; display: flex; flex-direction: column"></div>
        <script type="module" src="/src/index.tsx"></script>
        </body>
</html>
`;

                const expected = `<!DOCTYPE html>
<html lang="en">
    <head>
        <meta charset="utf-8" />
        <base href="@basePath" />
        <link rel="icon" href="assets/icons/favicon.ico" id="favicon-link" />
        <link rel="mask-icon" href="assets/icons/favicon.ico" id="mask-icon-link" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <meta name="theme-color" content="#000000" />
        <meta name="description" content="A Metadata Platform for the Modern Data Stack" />
        <!--
          manifest.json provides metadata used when your web app is installed on a
          user's mobile device or desktop. See https://developers.google.com/web/fundamentals/web-app-manifest/
        -->
        <link rel="manifest" href="manifest.json" id="manifest-link" />
		<script type="module" crossorigin="" src="assets/index-K056Uwn8.js"></script>
		<link rel="stylesheet" crossorigin="" href="assets/index-Byck28hc.css">
        <title>DataHub</title>
    </head>
    <body>
        <noscript>You need to enable JavaScript to run this app.</noscript>
        <div id="root" style="min-height: 100%; display: flex; flex-direction: column"></div>
        <script type="module" src="/src/index.tsx"></script>
        </body>
</html>`;

                expect(transformFn(input)).toBe(expected);
            });
        });
    });
});
