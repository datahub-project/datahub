# Chatbot evals

We have a bunch of test cases / prompts, tied to specific customer instances, alongside guidelines for what we expect the response to include.

To run the bot + evals:

1. `run.py` runs the chatbot on a set of prompts and saves the results to mlflow / local files.
2. `chat_review.py` allows you to review the results of the chatbot + runs LLM judge evals.

## Eval test case format

`prompts.yaml` contains a list of the prompts to run. Format is a list of dicts, where each entry has the following fields:

- `id`: a unique identifier for the prompt
- `instance`: the instance of the chatbot to run the prompt on
- `message`: the prompt message
- `response_guidelines`: the response guidelines for the prompt; passed to the LLM judge

## Other tools

- `chat_ui.py` is for running interactive chat sessions without needing to set up Slack.
