import argparse
import os
from glob import glob

import pandas as pd
from tqdm import tqdm

from ubi_sns_anal.const import COLUMN_NAME
from ubi_sns_anal.io import mkdir_p


TOPIC_KEYWORDS_MAPPING = {
    "cash_handout": (
        # General for 普發
        "普發現金",
        "現金普發",
        "全民普發",
        # Discussion for 紓困 4.0 in 2021
        "普發+紓困",
        "普發+10000",
        "普發+10,000",
        "普發+1萬",
        "普發+一萬",
        # 普發 6,000 元 in 2023
        "普發+6000",
        "普發+6,000",
        "普發+6千",
        "普發+六千",
    ),
    "voucher": (
        "消費券",
        "振興券",
        "三倍券",
        "五倍券",
        "熊好券",
        "藝文振興券",
        "藝FUN券",
        "文化成年禮金",
        "農遊券",
        "動滋券",
        "客庄券",
        "國旅券",
        "i原券",
        "好食券",
        "地方創生券",
    ),
    "elder_allowance": (
        "敬老金",
        "敬老+禮金",
        "長者+禮金",
        "敬老+慰問金",
        "長者+慰問金",
        "敬老津貼",
    )
}
TOPIC_KEYWORDS_MAPPING["all"] = [
    keyword
    for keywords in TOPIC_KEYWORDS_MAPPING.values()
    for keyword in keywords
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Script for extract useful articles from raw data",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "data",
        type=str,
        help="Path to directory that contains the raw data",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default="output",
        help="Path to output directory",
    )

    args = parser.parse_args()

    return args


def match_article(text, keywords):
    text = text.replace(" ", "")

    matched_keywords = []
    for keyword in keywords:
        sub_keywords = keyword.split("+")
        matched = all([sub_keyword in text for sub_keyword in sub_keywords])
        if matched:
            matched_keywords.append(keyword)

    return matched_keywords


def filter_target_articles(src_path, keywords):
    uid = os.path.splitext(os.path.basename(src_path))[0]
    df = pd.read_csv(src_path, dtype=str, keep_default_na=False)

    articles = []
    for _, info in df.iterrows():
        matched_keywords = match_article(info[COLUMN_NAME.TEXT], keywords)
        if matched_keywords:
            articles.append({
                COLUMN_NAME.UID: uid,
                COLUMN_NAME.TIME: info[COLUMN_NAME.TIME],
                COLUMN_NAME.TEXT: info[COLUMN_NAME.TEXT],
                COLUMN_NAME.KEYWORDS: matched_keywords,
            })

    return articles


def gather_target_articles(src_paths, keywords):
    articles = []
    for src_path in tqdm(src_paths):
        articles += filter_target_articles(src_path, keywords)

    df = pd.DataFrame(articles)
    df = df.sort_values(COLUMN_NAME.TIME)

    return df


def main(args):
    mkdir_p(args.output)

    src_paths = glob(os.path.join(args.data, "*.csv"))

    for topic, keywords in TOPIC_KEYWORDS_MAPPING.items():
        print(f"Gathering target articles for {topic}")
        df = gather_target_articles(src_paths, keywords)

        df.to_csv(os.path.join(args.output, f"{topic}.csv"), index=False)

    print(f"Results saved in {args.output}")


if __name__ == "__main__":
    main(parse_args())
