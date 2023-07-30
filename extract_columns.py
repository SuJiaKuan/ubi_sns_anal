import argparse
import os
from glob import glob

import pandas as pd

from ubi_sns_anal.const import COLUMN_NAME
from ubi_sns_anal.io import mkdir_p


SCRAPER_MAPPING = {
    "post_id": COLUMN_NAME.POST_ID,
    "text": COLUMN_NAME.TEXT,
    "time": COLUMN_NAME.TIME,
    "post_url": COLUMN_NAME.POST_URL,
}
EXTENSION_MAPPING = {
    "Post Id": COLUMN_NAME.POST_ID,
    "Content": COLUMN_NAME.TEXT,
    "Posted At": COLUMN_NAME.TIME,
    "Post Url": COLUMN_NAME.POST_URL,
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Script for extract import columns from raw data",
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


def extract_columns(src_path, output_dir):
    df = pd.read_csv(src_path)

    if list(SCRAPER_MAPPING.keys())[0] in list(df.columns):
        mapping = SCRAPER_MAPPING
    else:
        mapping = EXTENSION_MAPPING

    df = df[mapping].rename(columns=mapping)

    df.to_csv(
        os.path.join(output_dir, os.path.basename(src_path)),
        index=False,
    )


def main(args):
    mkdir_p(args.output)

    src_paths = glob(os.path.join(args.data, "*.csv"))
    for src_path in src_paths:
        extract_columns(src_path, args.output)

    print(f"Results saved in {args.output}")


if __name__ == "__main__":
    main(parse_args())
