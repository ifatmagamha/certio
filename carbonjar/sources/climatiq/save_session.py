import asyncio
from playwright.async_api import async_playwright
from pathlib import Path

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto("https://www.climatiq.io/data/explorer?region=TN")

        print("\n>>> Veuillez vous connecter manuellement dans la fenêtre...")
        input(">>> Une fois connecté, appuyez sur Entrée ici pour continuer...")

        # Sauvegarde session
        Path("auth").mkdir(exist_ok=True)
        await context.storage_state(path="auth/auth_state.json")
        await browser.close()
        print("Session enregistrée avec succès.")

asyncio.run(main())
