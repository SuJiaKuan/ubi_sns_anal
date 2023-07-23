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


def crawl_fb_posts(
    fb_id,
    cookies_path,
    output_path,
    pages=99999,
    timeout=3000,
    extra=False,
):
    extra_str = "" if extra else "--no-extra-requests"
    cmd = \
        f"facebook-scraper " \
        f"--filename {output_path} " \
        f"--pages {pages} " \
        f"--timeout {timeout} " \
        f"--cookies {cookies_path} " \
        f"{extra_str} " \
        f"{fb_id}"

    return os.system(cmd)
