import argparse
import os
import tempfile
from shutil import copyfile

import pandas as pd

from ubi_sns_anal.crawling import parse_fb_url
from ubi_sns_anal.crawling import crawl_fb_posts
from ubi_sns_anal.io import mkdir_p


def parse_args():
    parser = argparse.ArgumentParser(
        description="Script for crawling raw data in batch mode",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "lst",
        type=str,
        help="Path to .CSV file that lists the people for crawling",
    )
    parser.add_argument(
        "fb_cookies",
        type=str,
        help="Path to facebook cookies file (cookies.txt)",
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


def is_failed_msg(msg):
    return any([
        failed_text in msg.lower()
        for failed_text in ("exception", "error", "couldn't get any posts")
    ])


def crawl_fb(info, cookies_path, ok_dir, ng_dir):
    party = info["Party"]
    name = info["Name"]
    fb_url = info["Facebook ID"]

    shown_name = f"\"{party} {name}\""

    if not fb_url:
        print(f"Skip {shown_name} because its Facebook URL is not provided")
        return

    fb_id = parse_fb_url(fb_url)
    uid = f"{party}_{name}".replace("; ", "-").replace(" ", "")

    filename = f"{uid}.csv"
    ok_path = os.path.join(ok_dir, filename)

    if os.path.isfile(ok_path):
        print(f"Skip {shown_name} because it was crawled successfully")
        return

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = os.path.join(tmp_dir, filename)

        try:
            out_msg = crawl_fb_posts(fb_id, cookies_path, tmp_path)
            print(out_msg)

            if is_failed_msg(out_msg):
                raise RuntimeError()

            copyfile(tmp_path, os.path.join(ok_dir, filename))
            print(f"Success to crawl {shown_name}")
        except Exception:
            copyfile(tmp_path, os.path.join(ng_dir, filename))
            print(f"Fail to crawl {shown_name}")


def main(args):
    ok_dir = args.output
    ng_dir = os.path.join(args.output, "NG")

    for new_dir in (ok_dir, ng_dir):
        mkdir_p(new_dir)

    df_lst = pd.read_csv(args.lst, dtype=str, keep_default_na=False)
    infos = [info for _, info in df_lst.iterrows()]

    for info in infos:
        crawl_fb(info, args.fb_cookies, ok_dir, ng_dir)


if __name__ == "__main__":
    main(parse_args())
