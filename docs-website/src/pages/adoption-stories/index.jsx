import React, { useState } from "react";
import clsx from "clsx";
import { HtmlClassNameProvider, ThemeClassNames } from "@docusaurus/theme-common";
import Layout from "@theme/Layout";
import LearnItemCard from "./_components/LearnItemCard";
import styles from "./styles.module.scss";

import customerStoriesIndexes from "../../../customerStoriesIndexes.json";

function AdoptionStoriesListPageContent() {
  const companies = (customerStoriesIndexes?.companies || []).filter((company) => company.link);
  const [activeFilters, setActiveFilters] = useState([]);
  const categories = ["B2B & B2C", "Financial & Fintech", "E-Commerce", "And More"];

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

  return (
    <Layout>
      <header className={"hero"}>
        <div className="container">
          <div className="hero__content">
            <div>
              <h1 className="hero__title">DataHub Adoption Stories</h1>
              <p className="hero__subtitle">Meet the DataHub users who have shared their stories with us.</p>
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
            <LearnItemCard key={company.name} company={company} />
          ))}
        </div>
      </div>
    </Layout>
  );
}

export default function AdoptionStoriesListPage() {
  return (
      <AdoptionStoriesListPageContent />
  );
}
