import { useMemo } from 'react';
import type { SearchResponse } from '../api/types';

interface SearchStatsProps {
  results: SearchResponse | null;
  totalResults: number;
  query?: string;
}

interface EntityTypeStats {
  type: string;
  count: number;
  percentage: number;
}

function extractEntityType(urn: string): string {
  // URN format: urn:li:entityType:...
  const match = urn.match(/^urn:li:([^:]+):/);
  return match ? match[1] : 'unknown';
}

function formatEntityType(type: string): string {
  // Convert camelCase or snake_case to Title Case
  return type
    .replace(/([A-Z])/g, ' $1')
    .replace(/_/g, ' ')
    .trim()
    .split(' ')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join(' ');
}

export function SearchStats({ results, totalResults, query }: SearchStatsProps) {
  const downloadScoreData = (scatterData: Array<{ position: number; score: number }>) => {
    // Generate descriptive filename
    const timestamp = new Date().toISOString().slice(0, 16).replace('T', '_').replace(/:/g, '-');
    const sanitizedQuery = (query || 'search').replace(/[^a-z0-9]/gi, '_').slice(0, 30);
    const numResults = scatterData.length;

    // CSV format for Desmos, Excel, Google Sheets
    const header = 'Position,Score,Index';
    const rows = scatterData.map((d, idx) => `${d.position},${d.score},${idx + 1}`);
    const content = [header, ...rows].join('\n');
    const filename = `scores_${sanitizedQuery}_${numResults}results_${timestamp}.csv`;

    // Create blob and download
    const blob = new Blob([content], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', filename);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };
  const entityTypeStats = useMemo(() => {
    if (!results || results.searchResults.length === 0) return [];

    const typeCounts = new Map<string, number>();

    results.searchResults.forEach(result => {
      const type = extractEntityType(result.entity.urn);
      typeCounts.set(type, (typeCounts.get(type) || 0) + 1);
    });

    const stats: EntityTypeStats[] = Array.from(typeCounts.entries())
      .map(([type, count]) => ({
        type,
        count,
        percentage: (count / results.searchResults.length) * 100,
      }))
      .sort((a, b) => b.count - a.count);

    return stats;
  }, [results]);

  const scoreStats = useMemo(() => {
    if (!results || results.searchResults.length === 0) return null;

    const scores = results.searchResults
      .map(r => r.score)
      .filter((score): score is number => score !== null && score !== undefined);

    if (scores.length === 0) return null;

    const sortedScores = [...scores].sort((a, b) => a - b);
    const min = Math.min(...scores);
    const max = Math.max(...scores);
    const avg = scores.reduce((a, b) => a + b, 0) / scores.length;
    const median = sortedScores[Math.floor(scores.length / 2)];

    // Calculate standard deviation
    const variance = scores.reduce((sum, score) => sum + Math.pow(score - avg, 2), 0) / scores.length;
    const stdDev = Math.sqrt(variance);

    // Calculate percentiles
    const percentileIndex = (p: number) =>
      Math.floor((sortedScores.length - 1) * p);
    const p25 = sortedScores[percentileIndex(0.25)];
    const p75 = sortedScores[percentileIndex(0.75)];
    const p90 = sortedScores[percentileIndex(0.9)];

    // Create scatter plot data points (position vs score)
    const scatterData = results.searchResults
      .map((r, idx) => ({
        position: results.start + idx + 1,
        score: r.score || 0,
      }))
      .filter(d => d.score > 0);

    return { min, max, avg, median, stdDev, p25, p75, p90, count: scores.length, scatterData };
  }, [results]);

  if (!results || results.searchResults.length === 0) {
    return null;
  }

  const maxEntityCount = Math.max(...entityTypeStats.map(s => s.count));

  const statsCount = results.searchResults.length;
  const isSampled = statsCount < totalResults;

  return (
    <div className="search-stats">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
        <h3 style={{ margin: 0 }}>
          Search Statistics ({statsCount.toLocaleString()} of {totalResults.toLocaleString()} total results)
        </h3>
        {isSampled && (
          <span style={{ fontSize: '0.85em', color: '#666', fontStyle: 'italic' }}>
            Statistics based on current page results
          </span>
        )}
      </div>

      <div className="stats-grid">
        {/* Entity Type Distribution */}
        <div className="stat-section">
          <h4>Entity Type Distribution</h4>
          <div className="bar-chart">
            {entityTypeStats.map((stat) => (
              <div key={stat.type} className="bar-row">
                <div className="bar-label">{formatEntityType(stat.type)}</div>
                <div className="bar-container">
                  <div
                    className="bar-fill"
                    style={{ width: `${(stat.count / maxEntityCount) * 100}%` }}
                  >
                    <span className="bar-value">{stat.count}</span>
                  </div>
                </div>
                <div className="bar-percentage">{stat.percentage.toFixed(1)}%</div>
              </div>
            ))}
          </div>
        </div>

        {/* Score Distribution */}
        {scoreStats && (
          <div className="stat-section">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
              <h4 style={{ margin: 0 }}>Score Distribution</h4>
              <button
                onClick={() => downloadScoreData(scoreStats.scatterData)}
                style={{
                  padding: '6px 12px',
                  fontSize: '0.85em',
                  border: '1px solid #0066cc',
                  borderRadius: '4px',
                  backgroundColor: 'white',
                  color: '#0066cc',
                  cursor: 'pointer',
                  fontWeight: 500,
                }}
                title="Download as CSV for Desmos, Excel, or Google Sheets"
              >
                ⬇️ Download CSV
              </button>
            </div>
            <div className="score-summary">
              <div className="summary-row">
                <span className="summary-label">Min:</span>
                <span className="summary-value">{scoreStats.min.toFixed(3)}</span>
              </div>
              <div className="summary-row">
                <span className="summary-label">P25:</span>
                <span className="summary-value">{scoreStats.p25.toFixed(3)}</span>
              </div>
              <div className="summary-row">
                <span className="summary-label">Median:</span>
                <span className="summary-value">{scoreStats.median.toFixed(3)}</span>
              </div>
              <div className="summary-row">
                <span className="summary-label">P75:</span>
                <span className="summary-value">{scoreStats.p75.toFixed(3)}</span>
              </div>
              <div className="summary-row">
                <span className="summary-label">P90:</span>
                <span className="summary-value">{scoreStats.p90.toFixed(3)}</span>
              </div>
              <div className="summary-row">
                <span className="summary-label">Max:</span>
                <span className="summary-value">{scoreStats.max.toFixed(3)}</span>
              </div>
              <div className="summary-row">
                <span className="summary-label">Mean:</span>
                <span className="summary-value">{scoreStats.avg.toFixed(3)}</span>
              </div>
              <div className="summary-row">
                <span className="summary-label">Std Dev:</span>
                <span className="summary-value">{scoreStats.stdDev.toFixed(3)}</span>
              </div>
            </div>
            <div className="score-scatter-plot">
              <div className="scatter-plot-container">
                <svg viewBox="0 0 600 200" preserveAspectRatio="xMidYMid meet">
                  {/* Y-axis labels */}
                  <text x="35" y="15" fontSize="10" fill="#666" textAnchor="end">{scoreStats.max.toFixed(2)}</text>
                  <text x="35" y="105" fontSize="10" fill="#666" textAnchor="end">{((scoreStats.max + scoreStats.min) / 2).toFixed(2)}</text>
                  <text x="35" y="195" fontSize="10" fill="#666" textAnchor="end">{scoreStats.min.toFixed(2)}</text>

                  {/* Y-axis */}
                  <line x1="40" y1="10" x2="40" y2="190" stroke="#ccc" strokeWidth="1" />

                  {/* X-axis */}
                  <line x1="40" y1="190" x2="590" y2="190" stroke="#ccc" strokeWidth="1" />

                  {/* Grid lines */}
                  <line x1="40" y1="100" x2="590" y2="100" stroke="#f0f0f0" strokeWidth="1" strokeDasharray="2,2" />

                  {/* X-axis label */}
                  <text x="315" y="210" fontSize="11" fill="#666" textAnchor="middle" fontWeight="500">Ranking Position</text>

                  {/* Y-axis label */}
                  <text x="-100" y="15" fontSize="11" fill="#666" textAnchor="middle" fontWeight="500" transform="rotate(-90)">Score</text>

                  {/* Data points */}
                  {scoreStats.scatterData.map((point, idx) => {
                    const scoreRange = scoreStats.max - scoreStats.min;
                    const denominator = Math.max(scoreStats.scatterData.length - 1, 1);
                    const x = 40 + (idx / denominator) * 550;
                    const y = scoreRange > 0
                      ? 190 - ((point.score - scoreStats.min) / scoreRange) * 180
                      : 100;
                    return (
                      <circle
                        key={idx}
                        cx={x}
                        cy={y}
                        r="3"
                        fill="#0066cc"
                        opacity="0.6"
                      >
                        <title>Pos {point.position}: {point.score.toFixed(3)}</title>
                      </circle>
                    );
                  })}
                </svg>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
