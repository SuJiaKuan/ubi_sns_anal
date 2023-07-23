import os


_VALID_FB_URL_PREFIXS = (
    "https://www.facebook.com/profile.php?id=",
    "https://www.facebook.com/",
)


def parse_fb_url(fb_url):
    for valid_prefix in _VALID_FB_URL_PREFIXS:
        if fb_url.startswith(valid_prefix):
            return fb_url.split(valid_prefix)[1]

    raise ValueError(f"Invalid Facebook URL: {fb_url}")


def crawl_fb_posts(fb_id, output_path, pages=99999, timeout=3000):
    cmd = \
        f"facebook-scraper " \
        f"--filename {output_path} " \
        f"--pages {pages} " \
        f"--no-extra-requests " \
        f"--timeout {timeout} " \
        f"--cookies \"from_browser\" " \
        f"{fb_id}"

    return os.system(cmd)
