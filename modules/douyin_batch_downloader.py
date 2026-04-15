from __future__ import annotations

import argparse
import asyncio
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import httpx


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOUYIN_MODULE_ROOT = Path(__file__).resolve().parent / "douyin_api"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "runtime" / "douyin_downloads"
DOUYIN_WEB_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0.0.0 Safari/537.36"
)
DOUYIN_BROWSER_CAPTURE_TIMEOUT_MS = 20000

if str(DOUYIN_MODULE_ROOT) not in sys.path:
    sys.path.insert(0, str(DOUYIN_MODULE_ROOT))

HybridCrawlerClass = None


def get_hybrid_crawler_class():
    global HybridCrawlerClass
    if HybridCrawlerClass is None:
        from crawlers.hybrid.hybrid_crawler import HybridCrawler as LoadedHybridCrawler  # noqa: E402

        HybridCrawlerClass = LoadedHybridCrawler
    return HybridCrawlerClass


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download a Douyin/TikTok/Bilibili video to a local directory.")
    parser.add_argument("--url", required=True, help="Share URL from Douyin/TikTok/Bilibili.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory used to store downloaded files.")
    parser.add_argument("--output-name", default="", help="Optional output filename. Default: platform_videoid.mp4")
    parser.add_argument("--filename-prefix", default="", help="Optional filename prefix when output-name is not provided.")
    parser.add_argument("--with-watermark", action="store_true", help="Download the watermark version when available.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite an existing file with the same output name.")
    return parser.parse_args(argv)


def build_output_name(data: dict, with_watermark: bool, prefix: str) -> str:
    suffix = "_watermark" if with_watermark else ""
    return f"{prefix}{data['platform']}_{data['video_id']}{suffix}.mp4"


def strip_share_url_tail(value: str) -> str:
    text = str(value or "").strip()
    while text and text[-1] in "\"'<>)]}，。！？；：,.;!?" :
        text = text[:-1].rstrip()
    return text


def extract_share_url(raw_text: str) -> str:
    text = str(raw_text or "").strip()
    if not text:
        raise ValueError("缺少抖音分享链接。")

    direct_match = re.search(r"https?://[^\s]+", text, flags=re.IGNORECASE)
    if direct_match:
        return strip_share_url_tail(direct_match.group(0))

    host_match = re.search(
        r"(?:(?:v\.douyin\.com|www\.douyin\.com|douyin\.com|vm\.tiktok\.com|www\.tiktok\.com|m\.tiktok\.com|b23\.tv|www\.bilibili\.com)/[^\s]+)",
        text,
        flags=re.IGNORECASE,
    )
    if host_match:
        return strip_share_url_tail(f"https://{host_match.group(0)}")

    raise ValueError("无法从当前内容里提取有效的抖音分享链接，请粘贴整段分享文案或 http(s) 链接。")


def is_douyin_url(url: str) -> bool:
    host = urlparse(str(url or "")).netloc.lower()
    return "douyin.com" in host


def extract_douyin_video_id(url: str) -> str:
    text = str(url or "")
    for pattern in (
        r"/video/(\d+)",
        r"[?&]vid=(\d+)",
        r"[?&]aweme_id=(\d+)",
        r"[?&]__vid=(\d+)",
    ):
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    return ""


def clean_douyin_title(title: str) -> str:
    text = re.sub(r"\s*-\s*抖音\s*$", "", str(title or "").strip(), flags=re.IGNORECASE)
    return text.strip()


def select_best_douyin_video(candidates: list[dict]) -> dict | None:
    ranked: list[tuple[int, dict]] = []
    for item in candidates:
        url = str(item.get("url") or "")
        host = urlparse(url).netloc.lower()
        if not url:
            continue
        if "douyinstatic.com" in host or url.endswith("/uuu_265.mp4"):
            continue

        score = 0
        if "douyinvod.com" in host:
            score += 5
        if "__vid=" in url:
            score += 3
        if "mime_type=video_mp4" in url:
            score += 2
        if item.get("content_type", "").startswith("video/"):
            score += 1
        score += min(len(url) // 100, 5)
        ranked.append((score, item))

    if not ranked:
        return None
    ranked.sort(key=lambda pair: pair[0], reverse=True)
    return ranked[0][1]


def build_cookie_header(cookies: list[dict]) -> str:
    parts = []
    for cookie in cookies:
        name = cookie.get("name")
        value = cookie.get("value")
        if name and value is not None:
            parts.append(f"{name}={value}")
    return "; ".join(parts)


async def launch_douyin_browser(playwright):
    last_error = None
    for launch_kwargs in (
        {"channel": "msedge", "headless": True},
        {"headless": True},
    ):
        try:
            return await playwright.chromium.launch(**launch_kwargs)
        except Exception as exc:  # pragma: no cover - browser availability depends on host
            last_error = exc
    raise RuntimeError(f"Unable to launch a Chromium browser for Douyin fallback: {last_error}") from last_error


async def extract_douyin_via_browser(share_url: str) -> dict:
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:  # pragma: no cover - dependency issue
        raise RuntimeError("Playwright is required for the Douyin browser fallback.") from exc

    captured: list[dict] = []

    async with async_playwright() as playwright:
        browser = await launch_douyin_browser(playwright)
        context = await browser.new_context(
            user_agent=DOUYIN_WEB_USER_AGENT,
            viewport={"width": 1440, "height": 900},
            locale="zh-CN",
        )
        page = await context.new_page()

        def on_response(response) -> None:
            response_url = response.url
            content_type = response.headers.get("content-type", "")
            if response.status not in (200, 206):
                return
            if (
                "video/mp4" in content_type
                or "mime_type=video_mp4" in response_url
                or "__vid=" in response_url
            ):
                captured.append(
                    {
                        "url": response_url,
                        "status": response.status,
                        "content_type": content_type,
                    }
                )

        page.on("response", on_response)
        await page.goto(share_url, wait_until="domcontentloaded", timeout=90000)

        waited_ms = 0
        best_video = select_best_douyin_video(captured)
        while best_video is None and waited_ms < DOUYIN_BROWSER_CAPTURE_TIMEOUT_MS:
            await page.wait_for_timeout(1000)
            waited_ms += 1000
            best_video = select_best_douyin_video(captured)

        final_page_url = page.url
        title = await page.title()
        cookies = await context.cookies()
        await context.close()
        await browser.close()

    if best_video is None:
        raise RuntimeError("Douyin browser fallback opened the page but did not capture a playable mp4 stream.")

    video_id = extract_douyin_video_id(final_page_url) or extract_douyin_video_id(best_video["url"]) or "unknown"
    description = clean_douyin_title(title)
    download_headers = {
        "User-Agent": DOUYIN_WEB_USER_AGENT,
        "Referer": final_page_url,
    }
    cookie_header = build_cookie_header(cookies)
    if cookie_header:
        download_headers["Cookie"] = cookie_header

    return {
        "platform": "douyin",
        "type": "video",
        "video_id": video_id,
        "desc": description,
        "create_time": None,
        "author": None,
        "statistics": None,
        "video_data": {
            "wm_video_url": best_video["url"],
            "wm_video_url_HQ": best_video["url"],
            "nwm_video_url": best_video["url"],
            "nwm_video_url_HQ": best_video["url"],
        },
        "_download_headers": download_headers,
        "_fallback": "playwright_browser_capture",
    }


async def resolve_headers(crawler: HybridCrawler, platform: str) -> dict:
    if platform == "tiktok":
        return (await crawler.TikTokWebCrawler.get_tiktok_headers()).get("headers", {})
    if platform == "bilibili":
        return (await crawler.BilibiliWebCrawler.get_bilibili_headers()).get("headers", {})
    return (await crawler.DouyinWebCrawler.get_douyin_headers()).get("headers", {})


async def download_stream(url: str, headers: dict, output_path: Path) -> None:
    timeout = httpx.Timeout(connect=30.0, read=300.0, write=300.0, pool=300.0)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
        async with client.stream("GET", url, headers=headers) as response:
            response.raise_for_status()
            with output_path.open("wb") as fh:
                async for chunk in response.aiter_bytes():
                    if chunk:
                        fh.write(chunk)


async def download_bilibili_video(video_url: str, audio_url: str, headers: dict, output_path: Path) -> None:
    with tempfile.NamedTemporaryFile(suffix=".m4v", delete=False) as video_temp:
        video_temp_path = Path(video_temp.name)
    with tempfile.NamedTemporaryFile(suffix=".m4a", delete=False) as audio_temp:
        audio_temp_path = Path(audio_temp.name)

    try:
        await download_stream(video_url, headers, video_temp_path)
        await download_stream(audio_url, headers, audio_temp_path)

        command = [
            "ffmpeg",
            "-y",
            "-i",
            str(video_temp_path),
            "-i",
            str(audio_temp_path),
            "-c:v",
            "copy",
            "-c:a",
            "copy",
            "-f",
            "mp4",
            str(output_path),
        ]
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr.strip() or "ffmpeg merge failed")
    finally:
        video_temp_path.unlink(missing_ok=True)
        audio_temp_path.unlink(missing_ok=True)


def write_metadata(meta_path: Path, share_url: str, data: dict, output_path: Path) -> None:
    payload = {
        "share_url": share_url,
        "platform": data.get("platform"),
        "type": data.get("type"),
        "video_id": data.get("video_id"),
        "desc": data.get("desc"),
        "create_time": data.get("create_time"),
        "author": data.get("author"),
        "statistics": data.get("statistics"),
        "output_path": str(output_path),
    }
    meta_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


async def main_async(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    output_dir = Path(args.output_dir).expanduser().resolve()
    share_url = extract_share_url(args.url)
    crawler = get_hybrid_crawler_class()()

    if share_url != str(args.url).strip():
        print("NORMALIZED_SHARE_URL", share_url)

    if is_douyin_url(share_url):
        try:
            data = await extract_douyin_via_browser(share_url)
        except Exception as browser_exc:
            print("DOUYIN_BROWSER_FAILED", type(browser_exc).__name__, str(browser_exc))
            data = await crawler.hybrid_parsing_single_video(share_url, minimal=True)
    else:
        data = await crawler.hybrid_parsing_single_video(share_url, minimal=True)

    if data.get("type") != "video":
        raise RuntimeError("Only video links are supported in local batch mode right now.")

    output_name = args.output_name or build_output_name(data, args.with_watermark, args.filename_prefix)
    output_path = output_dir / output_name

    if output_path.exists() and not args.overwrite:
        print("SKIP_EXISTS", str(output_path))
        write_metadata(output_path.with_suffix(".json"), share_url, data, output_path)
        return 0

    headers = data.get("_download_headers") or await resolve_headers(crawler, data.get("platform", "douyin"))
    video_data = data.get("video_data") or {}

    if data.get("platform") == "bilibili":
        video_url = video_data.get("nwm_video_url_HQ")
        audio_url = video_data.get("audio_url")
        if not video_url or not audio_url:
            raise RuntimeError("Bilibili download is missing video or audio URL.")
        await download_bilibili_video(video_url, audio_url, headers, output_path)
    else:
        key = "wm_video_url_HQ" if args.with_watermark else "nwm_video_url_HQ"
        download_url = video_data.get(key) or video_data.get("nwm_video_url") or video_data.get("wm_video_url")
        if not download_url and data.get("platform") == "douyin":
            print("DOUYIN_URL_MISSING_RETRY_BROWSER")
            data = await extract_douyin_via_browser(share_url)
            headers = data.get("_download_headers") or headers
            video_data = data.get("video_data") or {}
            download_url = video_data.get(key) or video_data.get("nwm_video_url") or video_data.get("wm_video_url")
        if not download_url:
            raise RuntimeError("Unable to resolve a downloadable video URL from the share link.")
        await download_stream(download_url, headers, output_path)

    write_metadata(output_path.with_suffix(".json"), share_url, data, output_path)
    print("DOWNLOADED", str(output_path))
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(main_async(argv))


if __name__ == "__main__":
    sys.exit(main())
