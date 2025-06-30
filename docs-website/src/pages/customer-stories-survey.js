import React, { useEffect } from "react";

export default function Home() {
  useEffect(() => {
    window.location.href = "https://www.datahub.com/share-your-journey";
  }, []);

  return null;
}
