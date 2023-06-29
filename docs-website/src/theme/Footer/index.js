import React from "react";
import Footer from "@theme-original/Footer";
import MarkpromptHelp from "../../components/MarkpromptHelp";

export default function FooterWrapper(props) {
  return (
    <>
      <MarkpromptHelp />
      <Footer {...props} />
    </>
  );
}
