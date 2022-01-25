import argparse
import asyncio
import httpx
import json
import logging
import os

import jsonpath_ng as jsonpath
import prometheus_client as prom

LOG = logging.getLogger(__name__)


class Transformer:
    def __init__(self, config, interval=300):
        self.config = config
        self.interval = interval

        self.metrics = {}
        self.lock = asyncio.Lock()

        self.create_metrics()

    def create_metrics(self):
        defaults = self.config.get("defaults", {})
        labels = ["source"] + self.config.get("labels", [])
        for mspec in self.config["metrics"]:
            metric_name = f"{self.config.get('nameprefix', '')}{mspec['name']}"
            mtype = mspec.get("type", defaults.get("type", "gauge")).title()
            LOG.info("creating %s metric %s", mtype, metric_name)

            mclass = getattr(prom, mtype)
            self.metrics[metric_name] = mclass(
                metric_name, mspec.get("description", ""), labels
            )

    async def loop(self):
        while True:
            await self.update_metrics()
            await asyncio.sleep(self.interval)

    async def update_metrics(self):
        LOG.info("updating metrics")

        tasks = []
        for source in self.config["sources"]:
            for url in source["urls"]:
                tasks.append(asyncio.create_task(self.fetch_url(source, url)))

        results = await asyncio.gather(*tasks)
        for success, src, urlspec, data in results:
            if not success:
                LOG.info("skipping %s: failed", src["name"])
                continue

            labels = {"source": src["name"]}
            labels.update(urlspec.get("labels", {}))

            for mspec in src["metrics"]:
                metric_name = f"{self.config.get('nameprefix', '')}{mspec['name']}"
                expr = jsonpath.parse(mspec["path"])
                match = expr.find(data)
                if len(match) != 1:
                    LOG.error(
                        "found %d values for source %s metric %s",
                        len(match),
                        src["name"],
                        metric_name,
                    )
                    continue

                val = match[0].value
                self.metrics[metric_name].labels(**labels).set(val)
                LOG.debug("source %s metric %s = %s", src["name"], metric_name, val)

    async def fetch_url(self, src, urlspec):
        url = urlspec["url"].format(**os.environ)
        LOG.info("fetch source %s url %s", src["name"], url)
        async with httpx.AsyncClient() as client:
            try:
                res = await client.get(url)
                res.raise_for_status()
                success = True
            except httpx.HTTPStatusError as err:
                LOG.error("failed to fetch %s: %s", url, err)
                success = False

            return success, src, urlspec, res.json()


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--config-file",
        "-f",
        default=os.environ.get("JSON2PROM_CONFIG_FILE", "config.json"),
    )
    p.add_argument("--interval", "-i", type=int, default=300)
    p.add_argument("--port", "-p", type=int, default=8080)
    p.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=int(os.environ.get("J2P_LOG_LEVEL", "0")),
    )

    return p.parse_args()


def main():
    args = parse_args()

    loglevel = ["WARNING", "INFO", "DEBUG"][min(args.verbose, 2)]
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=loglevel,
    )

    with open(args.config_file) as fd:
        config = json.load(fd)

    T = Transformer(config, interval=args.interval)

    prom.start_http_server(args.port)
    asyncio.run(T.loop())


if __name__ == "__main__":
    main()
