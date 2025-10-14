import os
import pathlib
import subprocess
from datetime import datetime
from typing import Dict, Sequence

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from loguru import logger


def execute_notebook_save_as_html(
    notebook_path: pathlib.Path, output_dir: pathlib.Path, params: Dict[str, str]
) -> str:
    """
    Executes a Jupyter Notebook and saves the executed version as HTML.

    Args:
        notebook_path (str): The path to the Jupyter Notebook file (.ipynb).
        output_dir (str): The directory to save the executed notebook.

    Returns:
        str: The path to the successfully executed HTML notebook.

    Raises:
        FileNotFoundError: If the 'jupyter' command is not found.
        subprocess.CalledProcessError: If the notebook execution fails.
        Exception: For any other unexpected errors during execution.
    """
    notebook_filename = os.path.basename(notebook_path)
    notebook_name_without_ext, _ = os.path.splitext(notebook_filename)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    executed_notebook_filename = (
        f"{notebook_name_without_ext}_executed_{timestamp}.ipynb"
    )
    executed_notebook_path = os.path.join(output_dir, executed_notebook_filename)
    executed_notebook_filename_html = (
        f"{notebook_name_without_ext}_executed_{timestamp}.html"
    )
    executed_notebook_path_html = os.path.join(
        output_dir, executed_notebook_filename_html
    )

    os.makedirs(output_dir, exist_ok=True)

    logger.info(f"Executing notebook: {notebook_path}")

    param_options = []
    for k, v in params.items():
        param_options.append("-p")
        param_options.append(k)
        param_options.append(v)

    # Get the virtual environment Python executable
    # venv_path = os.path.join(os.path.dirname(__file__), "..", "..")
    papermill_command = [
        "papermill",
        str(notebook_path),
        str(executed_notebook_path),
        *param_options,
    ]
    jupyter_command = [
        "jupyter",
        "nbconvert",
        "--to",
        "html",
        "--no-input",
        str(executed_notebook_path),
        "--output",
        str(executed_notebook_path_html),
    ]
    subprocess.run(papermill_command, check=True)  # , cwd=venv_path
    subprocess.run(jupyter_command, check=True)  # , cwd=venv_path
    logger.info(f"Executed notebook saved to: {executed_notebook_path_html}")
    return executed_notebook_path_html


def plot_hist(
    df: pd.DataFrame,
    col_name: str,
    breakdown_col: str | None = None,
    title_suffix: str = "",
) -> None:
    if df is not None and col_name in df.columns:
        # Plot histogram of generation time
        plt.figure(figsize=(10, 6))
        bins: int | Sequence[float] | str | None = None
        if col_name == "generation_time_sec":
            bins = list(range(0, 301, 15))
        elif col_name == "span_execution_time_min":
            bins = np.arange(0, 11, 0.5).tolist()
        else:
            bins = 20

        if breakdown_col is not None and breakdown_col in df.columns:
            # Create stacked histogram colored by breakdown_col
            categories = df[breakdown_col].unique()
            colors = plt.cm.get_cmap("Set3")(
                range(len(categories))
            )  # Use a colormap for distinct colors

            # Create data for each category
            data_by_category = [
                df[df[breakdown_col] == cat][col_name] for cat in categories
            ]

            plt.hist(
                data_by_category,
                bins=bins,
                alpha=0.7,
                color=list(colors),
                edgecolor="black",
                label=list(categories),
                stacked=True,
            )
            plt.legend(title=breakdown_col)
        else:
            # Original single histogram
            plt.hist(
                df[col_name],
                bins=bins,
                alpha=0.7,
                color="skyblue",
                edgecolor="black",
            )

        plt.title(f"Distribution of {col_name} ({title_suffix})")
        plt.xlabel(f"{col_name}")
        plt.ylabel("Frequency")
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.show()

        # Print some statistics
        logger.info(f"{col_name} Statistics:")
        logger.info(f"Total rows: {df.shape[0]}")
        logger.info(f"Mean: {df[col_name].mean():.2f}")
        logger.info(f"Median: {df[col_name].median():.2f}")
        logger.info(f"P90: {df[col_name].quantile(0.9):.2f}")
        logger.info(f"Min: {df[col_name].min():.2f}")
        logger.info(f"Max: {df[col_name].max():.2f}")

        # Print breakdown statistics if breakdown_col is provided
        if breakdown_col is not None and breakdown_col in df.columns:
            logger.info(f"\nBreakdown by {breakdown_col}:")
            for cat in df[breakdown_col].unique():
                cat_data = df[df[breakdown_col] == cat][col_name]
                logger.info(
                    f"  {cat}: {len(cat_data)} rows, Mean: {cat_data.mean():.2f}, Median: {cat_data.median():.2f}, P90: {cat_data.quantile(0.9):.2f}"
                )
    else:
        logger.info(f"Could not find {col_name} column in the evaluation results.")


def plot_scatter(
    df: pd.DataFrame, x_col: str, y_col: str, breakdown_col: str | None = None
) -> None:
    """
    Create a scatter plot of two columns with optional coloring by a breakdown column.

    Args:
        df (pd.DataFrame): Input dataframe
        x_col (str): Name of column to plot on x-axis
        y_col (str): Name of column to plot on y-axis
        breakdown_col (str, optional): Name of categorical column to use for coloring
    """
    plt.figure(figsize=(10, 6))

    if breakdown_col is not None:
        # Get unique categories
        categories = df[breakdown_col].unique()

        # Create the scatter plot
        for category in categories:
            subset = df[df[breakdown_col] == category]
            plt.scatter(subset[x_col], subset[y_col], label=category)

    else:
        plt.scatter(df[x_col], df[y_col], alpha=0.6)
    plt.legend()
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    plt.title(f"{x_col} vs {y_col}")
    plt.grid(True, alpha=0.3)
    plt.show()
