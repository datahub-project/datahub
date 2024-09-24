import React from "react";
import { useEffect, useState } from "react";
import clsx from "clsx";
import { useBlogPost } from "@docusaurus/theme-common/internal";
import BlogPostItemContainer from "@theme/BlogPostItem/Container";
import BlogPostItemHeaderAuthors from "@theme/BlogPostItem/Header/Authors";
import BlogPostItemContent from "@theme/BlogPostItem/Content";
import BlogPostItemFooter from "@theme/BlogPostItem/Footer";
import HubspotForm from "../HubspotForm";
import styles from "./styles.module.scss";
import useLocalStorage from "../../../hooks/useLocalStorage";

function useContainerClassName() {
  const { isBlogPostPage } = useBlogPost();
  return !isBlogPostPage ? "margin-bottom--xl" : undefined;
}
export default function WebinarPostItem({ children, className }) {
  const containerClassName = useContainerClassName();
  const { metadata } = useBlogPost();
  const {
    title,
    status,
    gated,
    gate_form_id,
    embed_url,
    recurring,
    recurringLabel,
    sessions,
  } = metadata?.frontMatter;

  const [unlockedWebinars, setUnlockedWebinars] = useLocalStorage(
    "unlockedWebinars",
    []
  );

  const isUnlocked = !gated || unlockedWebinars?.includes(metadata?.permalink);

  const onMessage = (event) => {
    if (
      event.data.type === "hsFormCallback" &&
      event.data.id === gate_form_id
    ) {
      // When the Hubspot form is submitted, add the webinar id to the unlocked list in localStorage
      if (event.data.eventName === "onFormSubmitted") {
        setUnlockedWebinars([...unlockedWebinars, metadata?.permalink]);
      }
      // When the form is ready, set a hidden form field to the webinar title
      const hiddenField = document.querySelector(
        'input.hs-input[name="webinar"]'
      );
      if (event.data.eventName === "onFormReady" && hiddenField) {
        hiddenField.value = title;
      }
    }
  };

  useEffect(() => {
    window.addEventListener("message", onMessage);
    return () => {
      window.removeEventListener("message", onMessage);
    };
  });

  return (
    <BlogPostItemContainer className={clsx(containerClassName, className)}>
      <nav aria-label="breadcrumbs" style={{ marginBottom: "1rem" }}>
        <ul class="breadcrumbs">
          <li class="breadcrumbs__item">
            <a class="breadcrumbs__link" href="/webinars">
              Webinars
            </a>
          </li>

          <li
            class="breadcrumbs__item breadcrumbs__item--active"
            style={{ textTransform: "capitalize" }}
          >
            <span class="breadcrumbs__link">{status} Webinar</span>
          </li>
        </ul>
      </nav>
      <div>
        <h1>{title}</h1>

        {status === "upcoming" && (
          <div className={clsx("card", styles.sessionList)}>
            {sessions.map((session, idx) => (
              <div key={idx}>
                <span>{session.title}</span>
                {session?.register_url && (
                  <a
                    href={session?.register_url}
                    target="_blank"
                    className="button button--secondary"
                  >
                    Register
                  </a>
                )}
              </div>
            ))}
          </div>
        )}

        {status === "on-demand" && (
          <>
            {isUnlocked ? (
              <>
                <iframe
                  src={embed_url}
                  webkitAllowFullScreen
                  mozAllowFullScreen
                  allowFullScreen
                  className={clsx(styles.webinarIframe)}
                ></iframe>
              </>
            ) : (
              <div className="card">
                <h4>Watch Now</h4>
                <HubspotForm formId={gate_form_id} />
              </div>
            )}
          </>
        )}
      </div>
      <BlogPostItemContent>{children}</BlogPostItemContent>
      <hr />
      <h3>Presented by</h3>
      <BlogPostItemHeaderAuthors />
      <BlogPostItemFooter />
    </BlogPostItemContainer>
  );
}
