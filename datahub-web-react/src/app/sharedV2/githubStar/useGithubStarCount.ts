import { useEffect, useState } from 'react';

const GITHUB_REPO_API_URL = 'https://api.github.com/repos/datahub-project/datahub';
export const GITHUB_STAR_COUNT_CACHE_KEY = 'datahubGithubStarCount';
// GitHub allows 60 unauthenticated API calls per hour per IP.
export const GITHUB_STAR_COUNT_CACHE_TTL_MS = 24 * 60 * 60 * 1000;

type CachedStarCount = {
    count: number;
    fetchedAt: number;
};

// One shared request per page load. Failures are kept too, so installs that can't reach GitHub don't retry on every page.
let sessionRequest: Promise<number | null> | null = null;

function readCachedStarCount(): number | null {
    try {
        const raw = localStorage.getItem(GITHUB_STAR_COUNT_CACHE_KEY);
        if (!raw) return null;
        const cached: CachedStarCount = JSON.parse(raw);
        if (typeof cached.count !== 'number' || Date.now() - cached.fetchedAt > GITHUB_STAR_COUNT_CACHE_TTL_MS) {
            return null;
        }
        return cached.count;
    } catch {
        return null;
    }
}

function writeCachedStarCount(count: number) {
    try {
        const cached: CachedStarCount = { count, fetchedAt: Date.now() };
        localStorage.setItem(GITHUB_STAR_COUNT_CACHE_KEY, JSON.stringify(cached));
    } catch {
        // Storage may be full or blocked; the count is optional.
    }
}

function fetchStarCount(): Promise<number | null> {
    if (!sessionRequest) {
        sessionRequest = fetch(GITHUB_REPO_API_URL)
            .then((response) => (response.ok ? response.json() : null))
            .then((repo) => {
                const stars = repo?.stargazers_count;
                if (typeof stars !== 'number') return null;
                writeCachedStarCount(stars);
                return stars;
            })
            .catch(() => null);
    }
    return sessionRequest;
}

export default function useGithubStarCount(enabled: boolean): number | null {
    const [count, setCount] = useState<number | null>(() => (enabled ? readCachedStarCount() : null));

    useEffect(() => {
        if (!enabled) return undefined;
        const cached = readCachedStarCount();
        if (cached !== null) {
            setCount(cached);
            return undefined;
        }

        let isCancelled = false;
        fetchStarCount().then((stars) => {
            if (!isCancelled && stars !== null) setCount(stars);
        });
        return () => {
            isCancelled = true;
        };
    }, [enabled]);

    return count;
}
