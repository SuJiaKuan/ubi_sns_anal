import argparse
import os
from glob import glob

import pandas as pd
from tqdm import tqdm

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
        "data_dirs",
        type=str,
        nargs="+",
        help="Path to directories that contains the raw data",
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


def gather_same_paths(data_dirs):
    name2paths = {}
    for data_dir in data_dirs:
        for src_path in glob(os.path.join(data_dir, "*.csv")):
            name = os.path.basename(src_path)
            name2paths.setdefault(name, []).append(src_path)

    return list(name2paths.values())


def transform_columns(src_path):
    df = pd.read_csv(src_path)

    if list(SCRAPER_MAPPING.keys())[0] in list(df.columns):
        mapping = SCRAPER_MAPPING
    else:
        mapping = EXTENSION_MAPPING

    df = df[mapping].rename(columns=mapping)

    return df


def extract_columns(src_paths, output_dir):
    dfs = [transform_columns(src_path) for src_path in src_paths]
    df = pd.concat(dfs)
    df.index = df[COLUMN_NAME.TEXT].str.len()
    df = df.sort_index(ascending=False).reset_index(drop=True)
    df = df.drop_duplicates(subset=[COLUMN_NAME.POST_ID])
    df = df.sort_values(COLUMN_NAME.TIME)
    df = df.reset_index()

    df.to_csv(
        os.path.join(output_dir, os.path.basename(src_paths[0])),
        index=False,
    )


def main(args):
    mkdir_p(args.output)

    src_paths_lst = gather_same_paths(args.data_dirs)
    for src_paths in tqdm(src_paths_lst):
        extract_columns(src_paths, args.output)

    print(f"Results saved in {args.output}")


if __name__ == "__main__":
    main(parse_args())
