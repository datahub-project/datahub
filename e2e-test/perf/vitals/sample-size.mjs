export function measuredSampleNote(counts, target) {
    const builds = Object.keys(counts);
    const short = builds.some((build) => counts[build] < target);
    const listed = builds.map((build) => `${build} ${counts[build]}/${target}`).join(', ');
    return {
        line: `Measured runs in the table: ${listed}.`,
        warning: short ? '**Short of the configured run count. p50 uses only the runs that succeeded.**' : null,
    };
}
