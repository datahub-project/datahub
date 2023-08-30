import React, { useState } from "react";
import { supabase } from "./supabase";
import styles from "./styles.module.css";

const Feedback = ({ page }) => {
  const [reaction, setReaction] = useState(null);

  const handleReaction = async (selectedReaction) => {
    console.log("Button clicked:", selectedReaction);
    try {
      const { data, error } = await supabase.from("feedback").insert([
        {
          page: window.location.href,
          reaction: selectedReaction,
        },
      ]);

      if (error) {
        console.error("Error submitting feedback:", error);
        return;
      }

      setReaction(selectedReaction);
    } catch (error) {
      console.error("Error submitting feedback:", error);
    }
  };

  return (
    <div className={styles.feedbackWidget}>
      {reaction === null ? (
        <div className={styles.reactionButtons}>
          <h3>Is this page helpful?</h3>
          <button className={styles.reaction} onClick={() => handleReaction("thumbs_up")}>👍</button>
          <button className={styles.reaction} onClick={() => handleReaction("thumbs_down")}>👎</button>
        </div>
      ) : (
        <div className={styles.feedbackMessage}>
          Thanks for your feedback!
        </div>
      )}
    </div>
  );
};

export default Feedback;
