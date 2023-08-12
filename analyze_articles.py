import argparse
import os
from glob import glob

import pandas as pd
from tqdm import tqdm

from ubi_sns_anal.const import COLUMN_NAME
from ubi_sns_anal.sentiment import analyze_sentiment_gpt
from ubi_sns_anal.io import mkdir_p


def parse_args():
    parser = argparse.ArgumentParser(
        description="Script for analyze articles",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "data",
        type=str,
        help="Path to directory that contains the articles data",
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


def run_analysis(src_path):
    df = pd.read_csv(src_path, dtype=str, keep_default_na=False)

    gpt_sents = []
    for _, info in tqdm(df.iterrows(), total=df.shape[0]):
        gpt_sents.append(analyze_sentiment_gpt(info[COLUMN_NAME.TEXT]))

    df[COLUMN_NAME.SENTMENT_GPT] = gpt_sents

    return df


def main(args):
    mkdir_p(args.output)

    src_paths = glob(os.path.join(args.data, "*.csv"))

    for src_path in src_paths:
        print(f"Analyzing {src_path}")
        df = run_analysis(src_path)

        df.to_csv(
            os.path.join(args.output, os.path.basename(src_path)),
            index=False,
        )

    print(f"Results saved in {args.output}")


if __name__ == "__main__":
    main(parse_args())
