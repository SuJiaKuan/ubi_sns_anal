import os

import editdistance
import openai

from ubi_sns_anal.compute import memory
from ubi_sns_anal.const import SENTIMENT as SENT


openai.api_key = os.getenv("OPENAI_API_KEY")


def _get_gpt_completion(
    prompt,
    role="user",
    model="gpt-3.5-turbo",
    temperature=0,
):
    messages = [{
        "role": role,
        "content": prompt,
    }]
    response = openai.ChatCompletion.create(
        model=model,
        messages=messages,
        temperature=temperature,
    )

    return response.choices[0].message["content"]


@memory.cache
def _analyze_sentiment_gpt(text):
    prompt = \
        f"這篇政治人物的社群貼文情緒為何？\n" \
        f"社群貼文的內容會以三個連續的反引號分隔\n" \
        f"請只回答 '{SENT.POSITIVE}'、'{SENT.NEGATIVE}' 或是 '{SENT.NEUTRAL}'\n" \
        f"```\n" \
        f"{text}\n" \
        f"```"
    response = _get_gpt_completion(prompt)

    return response


def analyze_sentiment_gpt(text, max_len=2850):
    response = _analyze_sentiment_gpt(text[:max_len])
    result = min(
        [SENT.POSITIVE, SENT.NEGATIVE, SENT.NEUTRAL],
        key=lambda label: editdistance.eval(response, label)
    )

    return result
