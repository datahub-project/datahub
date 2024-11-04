import React from "react";
import { useLocation } from "react-router-dom"; // Import useLocation from react-router-dom
import Footer from "@theme-original/Footer";
import MarkpromptHelp from "../../components/MarkpromptHelp";

export default function FooterWrapper(props) {
  const location = useLocation(); // Get the current location
  const isDocsPage = location.pathname.startsWith("/docs"); // Check if the path starts with /docs

  return (
    <>
      {isDocsPage && <MarkpromptHelp />}
      <Footer {...props} />
    </>
  );
}
