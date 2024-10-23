import asyncio
from os import environ

import aiofiles
import httpx
import stamina
from logzero import logger


@stamina.retry(on=httpx.HTTPError, attempts=3)
async def fetch_url(client, url, headers):
    response = await client.get(url=url, headers=headers)
    response.raise_for_status()
    logger.info(f"{url} returned {response.status_code} after {response.elapsed}")
    return response


async def follow_and_extract(client, url, headers):
    request = await fetch_url(client, url, headers)
    try:
        next_url = request.links["next"].get("url")
        return request.text + await follow_and_extract(client, next_url, headers)
    except KeyError:
        return request.text


async def download_and_write_bib(
    client, zotero_headers, zotero_url, file_name="zotero.bib"
):
    zotero_connection = await fetch_url(client, zotero_url, zotero_headers)

    if zotero_connection.status_code == 403:
        logger.error("Access to library not granted.")
        return

    try:
        async with aiofiles.open(
            f"bibliography/{file_name}-last-modified-version", "r"
        ) as file:
            cached_version = int(await file.readline())
            logger.info(f"last-modified-version is {cached_version}")
    except (FileNotFoundError, ValueError):
        cached_version = 0

    latest_version = int(zotero_connection.headers.get("last-modified-version"))

    if cached_version == latest_version:
        logger.info(
            f"online version {latest_version} is not different from cache {cached_version}. Done!"
        )
        return

    logger.info(
        f"online version {latest_version} is different from cache {cached_version}. Fetching data..."
    )

    biblatex_file_content = await follow_and_extract(
        client, url=zotero_url, headers=zotero_headers
    )

    async with aiofiles.open(f"bibliography/{file_name}", "w") as file:
        await file.write(biblatex_file_content)
        logger.info(f"{file_name} updated")

    async with aiofiles.open(
        f"bibliography/{file_name}-last-modified-version", "w"
    ) as file:
        await file.write(str(latest_version))
        logger.info(f"last-modified-version updated to {latest_version}")


async def main():
    timeout = httpx.Timeout(10.0, connect=60.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        zotero_user_id = environ.get("ZOTERO_USER_ID")
        if zotero_user_id is None:
            logger.error("ZOTERO_USER_ID not set in GitHub secrets")
            return

        zotero_bearer_token = environ.get("ZOTERO_BEARER_TOKEN")
        if zotero_bearer_token is None:
            logger.error("ZOTERO_BEARER_TOKEN not set in GitHub secrets")
            return

        zotero_headers = {"Authorization": f"Bearer {zotero_bearer_token}"}
        zotero_user_url = (
            f"https://api.zotero.org/users/{zotero_user_id}/items?v=3&format=biblatex"
        )

        await download_and_write_bib(client, zotero_headers, zotero_user_url)

        logger.info("Downloading all groups!")

        groups_response = await fetch_url(
            client,
            f"https://api.zotero.org/users/{zotero_user_id}/groups/",
            zotero_headers,
        )
        groups = groups_response.json()

        for group in groups:
            group_id = group.get("id")
            if group_id:
                zotero_group_url = f"https://api.zotero.org/groups/{group_id}/items?v=3&format=biblatex"
                await download_and_write_bib(
                    client, zotero_headers, zotero_group_url, f"{group_id}.bib"
                )

        logger.info("Done!")


# Entry point for the script
if __name__ == "__main__":
    asyncio.run(main())
