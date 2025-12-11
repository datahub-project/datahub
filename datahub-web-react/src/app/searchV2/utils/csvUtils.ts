/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export function downloadFile(data: string, title: string) {
    const blobx = new Blob([data], { type: 'text/plain' }); // ! Blob
    const elemx = window.document.createElement('a');
    elemx.href = window.URL.createObjectURL(blobx); // ! createObjectURL
    elemx.download = title;
    elemx.style.display = 'none';
    document.body.appendChild(elemx);
    elemx.click();
    document.body.removeChild(elemx);
}

function createCsvContents(fieldNames: string[], rows: string[][]): string {
    let contents = `${fieldNames.join(',')}\n`;
    rows.forEach((row) => {
        // quotes need to be escaped for csvs -> " becomes ""
        contents = contents.concat(`${row.map((rowEl) => `"${rowEl.replace(/"/g, '""')}"`).join(',')}\n`);
    });

    return contents;
}

export function downloadRowsAsCsv(fieldNames: string[], rows: string[][], title: string) {
    const csvFileContents = createCsvContents(fieldNames, rows);
    downloadFile(csvFileContents, title);
}
