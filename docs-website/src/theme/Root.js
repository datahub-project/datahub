import React, { useEffect } from 'react';
import { useLocation } from '@docusaurus/router';

export default function Root({ children }) {
  const location = useLocation();

  useEffect(() => {
    if (location.hash) {
        console.log("Hash found:", location.hash);
        const id = location.hash.replace('#', '');
        const el = document.getElementById(id);
        console.log("Element:", el);
      if (el) {
        setTimeout(() => {
          el.scrollIntoView({ behavior: 'auto' });
        }, 0);
      }
    }
  }, [location]);

  return <>{children}</>;
}
