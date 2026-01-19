import { useState } from 'react';

interface JsonViewerProps {
  data: any;
}

interface JsonNodeProps {
  data: any;
  depth?: number;
  name?: string;
  isLast?: boolean;
}

function JsonNode({ data, depth = 0, name, isLast = true }: JsonNodeProps) {
  const [expanded, setExpanded] = useState(depth < 2); // Auto-expand first 2 levels

  const indent = depth * 20;

  if (data === null) {
    return (
      <div className="json-line" style={{ paddingLeft: `${indent}px` }}>
        {name && <span className="json-key">"{name}": </span>}
        <span className="json-null">null</span>
        {!isLast && <span className="json-comma">,</span>}
      </div>
    );
  }

  if (typeof data === 'boolean') {
    return (
      <div className="json-line" style={{ paddingLeft: `${indent}px` }}>
        {name && <span className="json-key">"{name}": </span>}
        <span className="json-boolean">{data.toString()}</span>
        {!isLast && <span className="json-comma">,</span>}
      </div>
    );
  }

  if (typeof data === 'number') {
    return (
      <div className="json-line" style={{ paddingLeft: `${indent}px` }}>
        {name && <span className="json-key">"{name}": </span>}
        <span className="json-number">{data}</span>
        {!isLast && <span className="json-comma">,</span>}
      </div>
    );
  }

  if (typeof data === 'string') {
    return (
      <div className="json-line" style={{ paddingLeft: `${indent}px` }}>
        {name && <span className="json-key">"{name}": </span>}
        <span className="json-string">"{data}"</span>
        {!isLast && <span className="json-comma">,</span>}
      </div>
    );
  }

  if (Array.isArray(data)) {
    const isEmpty = data.length === 0;

    if (isEmpty) {
      return (
        <div className="json-line" style={{ paddingLeft: `${indent}px` }}>
          {name && <span className="json-key">"{name}": </span>}
          <span className="json-bracket">[]</span>
          {!isLast && <span className="json-comma">,</span>}
        </div>
      );
    }

    return (
      <div>
        <div
          className="json-line json-expandable"
          style={{ paddingLeft: `${indent}px` }}
          onClick={() => setExpanded(!expanded)}
        >
          <span className="json-expand-icon">{expanded ? '▼' : '▶'}</span>
          {name && <span className="json-key">"{name}": </span>}
          <span className="json-bracket">[</span>
          {!expanded && (
            <>
              <span className="json-ellipsis">...</span>
              <span className="json-bracket">]</span>
              <span className="json-count"> ({data.length} items)</span>
            </>
          )}
        </div>
        {expanded && (
          <>
            {data.map((item, index) => (
              <JsonNode
                key={index}
                data={item}
                depth={depth + 1}
                isLast={index === data.length - 1}
              />
            ))}
            <div className="json-line" style={{ paddingLeft: `${indent}px` }}>
              <span className="json-bracket">]</span>
              {!isLast && <span className="json-comma">,</span>}
            </div>
          </>
        )}
      </div>
    );
  }

  if (typeof data === 'object') {
    const keys = Object.keys(data);
    const isEmpty = keys.length === 0;

    if (isEmpty) {
      return (
        <div className="json-line" style={{ paddingLeft: `${indent}px` }}>
          {name && <span className="json-key">"{name}": </span>}
          <span className="json-brace">{'{}'}</span>
          {!isLast && <span className="json-comma">,</span>}
        </div>
      );
    }

    return (
      <div>
        <div
          className="json-line json-expandable"
          style={{ paddingLeft: `${indent}px` }}
          onClick={() => setExpanded(!expanded)}
        >
          <span className="json-expand-icon">{expanded ? '▼' : '▶'}</span>
          {name && <span className="json-key">"{name}": </span>}
          <span className="json-brace">{'{'}</span>
          {!expanded && (
            <>
              <span className="json-ellipsis">...</span>
              <span className="json-brace">{'}'}</span>
              <span className="json-count"> ({keys.length} keys)</span>
            </>
          )}
        </div>
        {expanded && (
          <>
            {keys.map((key, index) => (
              <JsonNode
                key={key}
                data={data[key]}
                depth={depth + 1}
                name={key}
                isLast={index === keys.length - 1}
              />
            ))}
            <div className="json-line" style={{ paddingLeft: `${indent}px` }}>
              <span className="json-brace">{'}'}</span>
              {!isLast && <span className="json-comma">,</span>}
            </div>
          </>
        )}
      </div>
    );
  }

  return null;
}

export function JsonViewer({ data }: JsonViewerProps) {
  return (
    <div className="json-viewer">
      <JsonNode data={data} depth={0} />
    </div>
  );
}
