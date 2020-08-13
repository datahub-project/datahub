import decompress = require('decompress');
import * as fs from 'fs';
import * as http from 'http';
import * as lockfile from 'lockfile';
import * as mkdirp from 'mkdirp';
import * as path from 'path';

export function getContent(url: string, cacheFile: string): Promise<string> {
  return getBinary(url, cacheFile).then(file => fs.readFileSync(file).toString());
}

/**
 * Load the file from the url or local cache.
 *
 * @param url download url
 * @param cacheFile local file name to read from/save to
 */
export function getBinary(url: string, cacheFile: string): Promise<string> {
  const localFolder = path.dirname(cacheFile);
  if (fs.existsSync(cacheFile)) {
    return Promise.resolve(cacheFile);
  }
  mkdirp.sync(localFolder);
  const tempFile = `${cacheFile}-dload-${process.pid}`;
  if (fs.existsSync(tempFile)) {
    fs.unlinkSync(tempFile);
  }
  return new Promise((resolve, reject) => {
    // select http or https module, depending on reqested url
    const request = http.get(url, response => {
      // handle http errors
      if (!response.statusCode || response.statusCode < 200 || response.statusCode > 299) {
        if (fs.existsSync(tempFile)) {
          fs.unlinkSync(tempFile);
        }
        reject(new Error('Failed to load page, status code: ' + response.statusCode));
      }
      const stream = fs.createWriteStream(tempFile);
      response.on('data', chunk => stream.write(chunk));
      response.on('end', () => {
        stream.end();
      });

      // after .end() the stream will notify 'finish'
      stream.on('finish', () => {
        fs.renameSync(tempFile, cacheFile);
        resolve(cacheFile);
      });
      stream.on('error', err => reject(err));
    });
    // handle connection errors of the request
    request.on('error', err => {
      if (fs.existsSync(tempFile)) {
        fs.unlinkSync(tempFile);
      }
      reject(`Error downloading ${url}: ${err}`);
    });
  });
}

export interface Entry {
  path: string;
  data: Buffer;
  type: string;
}

export function unzip(archive: string, destination: string): Promise<Entry[]> {
  return new Promise((resolve, reject) => {
    const lck = destination + '.lock';
    lockfile.lock(lck, err => {
      if (err) {
        reject(`Cannot create lock file ${lck}: ${err}`);
        return;
      }

      decompress(archive, destination)
        .then((entries: Entry[]) => {
          lockfile.unlockSync(lck);
          resolve(entries);
        })
        .catch((dErr: any) => {
          lockfile.unlockSync(lck);
          reject(`Error decompressing ${archive}: ${dErr}`);
        });
    });
  });
}
