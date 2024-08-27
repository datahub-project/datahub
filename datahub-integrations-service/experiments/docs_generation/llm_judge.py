import click
import papermill


@click.command()
@click.argument("csv1_path", type=click.Path(exists=True, dir_okay=False))
@click.argument("csv2_path", type=click.Path(exists=True, dir_okay=False))
def main(csv1_path, csv2_path):
    # TODO: Execute directly instead of using papermill.
    papermill.execute_notebook(
        "LLM_Judge.ipynb",
        "output/LLM_Judge.ipynb",
        parameters=dict(
            csv1_path=csv1_path,
            csv2_path=csv2_path,
            testing_mode=False,
        ),
        progress_bar=True,
    )


if __name__ == "__main__":
    main()
