import argparse
import os
from glob import glob

import pandas as pd
from tqdm import tqdm

from ubi_sns_anal.const import COLUMN_NAME
from ubi_sns_anal.io import mkdir_p


KEYWORDS = (
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
)


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


def match_article(text):
    text = text.replace(" ", "")

    matched_keywords = []
    for keyword in KEYWORDS:
        sub_keywords = keyword.split("+")
        matched = all([sub_keyword in text for sub_keyword in sub_keywords])
        if matched:
            matched_keywords.append(keyword)

    return matched_keywords


def filter_target_articles(src_path):
    uid = os.path.splitext(os.path.basename(src_path))[0]
    df = pd.read_csv(src_path, dtype=str, keep_default_na=False)

    articles = []
    for _, info in df.iterrows():
        matched_keywords = match_article(info[COLUMN_NAME.TEXT])
        if matched_keywords:
            articles.append({
                COLUMN_NAME.UID: uid,
                COLUMN_NAME.TIME: info[COLUMN_NAME.TIME],
                COLUMN_NAME.TEXT: info[COLUMN_NAME.TEXT],
                COLUMN_NAME.KEYWORDS: matched_keywords,
            })

    return articles


def gather_target_articles(src_paths):
    articles = []
    for src_path in tqdm(src_paths):
        articles += filter_target_articles(src_path)

    df = pd.DataFrame(articles)
    df = df.sort_values(COLUMN_NAME.TIME)

    return df


def main(args):
    mkdir_p(args.output)

    src_paths = glob(os.path.join(args.data, "*.csv"))

    print("Gathering target articles...")
    df = gather_target_articles(src_paths)

    df.to_csv(os.path.join(args.output, "analysis_results.csv"), index=False)

    print(f"Results saved in {args.output}")


if __name__ == "__main__":
    main(parse_args())
