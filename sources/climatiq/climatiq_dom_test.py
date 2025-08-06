import asyncio
import json
import logging
from pathlib import Path
from playwright.async_api import async_playwright


class ClimatiqScraper:
    def __init__(self, region, headless=True):
        self.region = region
        self.base_url = f"https://www.climatiq.io/data/explorer?region={region}&data_version=%5E24"
        self.headless = headless
        self.output_path = Path(f"data/raw/climatiq_{region}_activities.json")
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    async def expand_all(self, page):
        logging.info("[EXPAND] Expanding all activity blocks")
        idx = 0
        while True:
            try:
                button = page.locator(f"#activity-row-{idx} >> role=button").filter(has_text="")
                if await button.count() == 0:
                    break
                await button.first.click()
                idx += 1
            except Exception as e:
                logging.warning(f"[EXPAND] Skipped activity-row-{idx}: {e}")
                break

    async def extract_activities(self, page):
        logging.info("[SCRAPE] Extracting activity names and info links")
        rows = page.locator("[role=row]")
        count = await rows.count()
        activities = []

        for i in range(count):
            try:
                row = rows.nth(i)

                name_el = row.locator("div.factors-table-cell--name")
                await name_el.wait_for(timeout=3000)
                name = await name_el.inner_text()

                info_el = row.locator("a:has-text('Info')")
                href = await info_el.get_attribute("href")

                if name and href:
                    activities.append({
                        "activity_name": name.strip(),
                        "info_link": f"https://www.climatiq.io{href.strip()}"
                    })

            except Exception as e:
                logging.warning(f"[SCRAPE] Failed to extract row {i}: {e}")

        logging.info(f"[SCRAPE] Extracted {len(activities)} activities")
        return activities


    async def run(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(storage_state="auth/auth_state.json")
            page = await context.new_page()

            logging.info(f"[NAVIGATE] {self.base_url}")
            await page.goto(self.base_url, timeout=60000)

            await self.expand_all(page)

            activities = await self.extract_activities(page)

            logging.info(f"[SAVE] Writing {len(activities)} entries to {self.output_path}")
            with self.output_path.open("w", encoding="utf-8") as f:
                json.dump(activities, f, indent=2, ensure_ascii=False)

            await browser.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scraper = ClimatiqScraper(region="TN", headless=True)
    asyncio.run(scraper.run())
