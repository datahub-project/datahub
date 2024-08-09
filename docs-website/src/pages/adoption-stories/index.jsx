import React, { useState, useEffect, useRef } from "react";
import Layout from "@theme/Layout";
import BrowserOnly from "@docusaurus/BrowserOnly";
import LearnItemCard from "./_components/LearnItemCard";
import styles from "./styles.module.scss";

import customerStoriesIndexes from "../../../adoptionStoriesIndexes.json";

function AdoptionStoriesListPageContent() {
  const companies = (customerStoriesIndexes?.companies || []).filter((company) => company.link);
  const [activeFilters, setActiveFilters] = useState([]);
  const categories = ["B2B & B2C", "E-Commerce", "Financial & Fintech", "And More"];
  const selectedCardRef = useRef(null);

  const filteredItems = activeFilters.length
    ? companies.filter((company) => activeFilters.includes(company.category))
    : companies;

  const handleFilterToggle = (category) => {
    if (activeFilters.includes(category)) {
      setActiveFilters(activeFilters.filter((filter) => filter !== category));
    } else {
      setActiveFilters([...new Set([...activeFilters, category])]);
    }
  };

  useEffect(() => {
    const selectedSlug = window.location.hash.substring(1);
    if (selectedCardRef.current) {
      selectedCardRef.current.scrollIntoView({ behavior: "smooth", block: "start", inline: "nearest" });
    }
  }, [selectedCardRef]);

  return (
    <Layout>
      <header className={"hero"}>
        <div className="container">
          <div className="hero__content">
            <div>
              <h1 className="hero__title">DataHub Adoption Stories</h1>
              <p className="hero__subtitle">Learn how the best data and AI teams are using DataHub
              <br />
              Check out more stories on the <a href="https://www.youtube.com/playlist?list=PLdCtLs64vZvGCKMQC2dJEZ6cUqWsREbFi" style={{ color: "black" }}>DataHub Youtube</a>.
              </p>
            </div>
          </div>
          <div className={styles.filterBar}>
            <strong>For: </strong>
            {categories.map((category) => (
              <button
                key={category}
                className={`button button--secondary ${activeFilters.includes(category) ? "button--active" : ""}`}
                onClick={() => handleFilterToggle(category)}
              >
                {category}
              </button>
            ))}
          </div>
        </div>
      </header>
      <div className="container">
        <div className="row">
          {filteredItems.map((company) => (
            <LearnItemCard
              key={company.name}
              company={company}
              isSelected={company.slug === window.location.hash.substring(1)}
              ref={company.slug === window.location.hash.substring(1) ? selectedCardRef : null}
            />
          ))}
        </div>
      </div>
    </Layout>
  );
}

export default function AdoptionStoriesListPage() {
  return (
    <BrowserOnly>
      {() => <AdoptionStoriesListPageContent />}
    </BrowserOnly>
  );
}
