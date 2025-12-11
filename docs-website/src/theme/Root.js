/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

import React, { useEffect } from "react";
import { useLocation } from "@docusaurus/router";

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

  // Transform images to use modal functionality
  useEffect(() => {
    const transformImages = () => {
      // Find all images in the markdown content
      const images = document.querySelectorAll(
        '.markdown img, article img, [class*="markdown"] img'
      );

      images.forEach((img) => {
        // Skip if already processed
        if (img.dataset.modalProcessed) return;
        img.dataset.modalProcessed = "true";

        // Make image clickable
        img.style.cursor = "zoom-in";
        img.addEventListener("click", () => {
          openImageModal(img);
        });

        // Add keyboard support
        img.setAttribute("tabindex", "0");
        img.setAttribute("role", "button");
        img.setAttribute(
          "aria-label",
          `Click to enlarge: ${img.alt || img.title || "image"}`
        );
        img.addEventListener("keydown", (e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            openImageModal(img);
          }
        });
      });
    };

    const openImageModal = (img) => {
      // Create modal overlay
      const modal = document.createElement("div");
      modal.style.cssText = `
        position: fixed;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background-color: rgba(0, 0, 0, 0.9);
        display: flex;
        align-items: center;
        justify-content: center;
        z-index: 9999;
        padding: 20px;
        animation: fadeIn 0.2s ease;
      `;

      // Create modal content
      const modalContent = document.createElement("div");
      modalContent.style.cssText = `
        position: relative;
        max-width: 90vw;
        max-height: 90vh;
        display: flex;
        flex-direction: column;
        align-items: center;
      `;

      // Modal image
      const modalImg = document.createElement("img");
      modalImg.src = img.src;
      modalImg.alt = img.alt || "";
      modalImg.title = img.title || "";
      modalImg.style.cssText = `
        max-width: 100%;
        max-height: 85vh;
        width: auto;
        height: auto;
        object-fit: contain;
        border-radius: 4px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.5);
        cursor: zoom-out;
      `;
      // Clicking the image closes the modal
      modalImg.addEventListener("click", () => closeModal());

      // Title if present
      let titleDiv = null;
      if (img.title || img.alt) {
        titleDiv = document.createElement("div");
        titleDiv.textContent = img.title || img.alt;
        titleDiv.style.cssText = `
          color: white;
          margin-top: 15px;
          text-align: center;
          font-size: 16px;
          max-width: 100%;
          padding: 0 20px;
        `;
      }

      // Assemble modal
      modalContent.appendChild(modalImg);
      if (titleDiv) modalContent.appendChild(titleDiv);
      modal.appendChild(modalContent);
      document.body.appendChild(modal);

      // Close handlers
      const closeModal = () => {
        modal.remove();
        document.removeEventListener("keydown", handleEscape);
      };

      const handleEscape = (e) => {
        if (e.key === "Escape") closeModal();
      };

      modal.addEventListener("click", (e) => {
        if (e.target === modal) closeModal();
      });

      document.addEventListener("keydown", handleEscape);
    };

    // Run transformation after a short delay to ensure DOM is ready
    const timer = setTimeout(transformImages, 100);

    // Also run on navigation
    transformImages();

    return () => clearTimeout(timer);
  }, [location]);

  return <>{children}</>;
}
