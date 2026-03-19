from __future__ import annotations


def render_page_html(url: str, timeout_ms: int = 45000) -> str:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError("Playwright is not installed. Install project dependencies and run 'playwright install chromium'.") from exc

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="networkidle", timeout=timeout_ms)
        html = page.content()
        browser.close()
    return html
