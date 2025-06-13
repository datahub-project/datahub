import React, { useEffect } from 'react';
import { useLocation } from '@docusaurus/router';

export default function Root({ children }) {
  const location = useLocation();

  useEffect(() => {
    setTimeout(() => {
      if (location.hash) {
        const id = decodeURIComponent(location.hash.substring(1));
        const el = document.getElementById(id);
        if (el) el.scrollIntoView();
      }
    }, 0);
  }, []);

  return <>{children}</>;
}
